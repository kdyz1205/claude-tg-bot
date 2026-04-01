"""
TFTA-HFT FragmentHybrid — matches singularity_engine bundled train.py for inference.
Loads arch.json + model.pt from a harness run directory.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


def build_fragment_mask(T: int, frag: int, device):
    import torch

    idx = torch.arange(T, device=device)
    bi = idx // max(1, int(frag))
    same = bi.unsqueeze(0) == bi.unsqueeze(1)
    m = torch.zeros(T, T, device=device)
    m[~same] = float("-inf")
    return m


def _build_model(hp: dict, dropout: float):
    import torch
    import torch.nn as nn

    d_model = int(hp["d_model"])
    n_heads = int(hp["n_heads"])
    n_layers = int(hp["transformer_layers"])

    class FragmentHybrid(nn.Module):
        def __init__(self):
            super().__init__()
            self.proj = nn.Linear(5, d_model)
            self.lstm = nn.LSTM(d_model, d_model, batch_first=True, num_layers=1)
            self.mha = nn.MultiheadAttention(
                d_model, n_heads, dropout=dropout, batch_first=True
            )
            enc = nn.TransformerEncoderLayer(
                d_model=d_model,
                nhead=n_heads,
                dim_feedforward=d_model * 4,
                dropout=dropout,
                batch_first=True,
                activation="gelu",
            )
            self.tr = nn.TransformerEncoder(enc, num_layers=n_layers)
            self.norm = nn.LayerNorm(d_model)
            self.head = nn.Linear(d_model, 1)

        def forward(self, x, frag_mask):
            x = self.proj(x)
            x, _ = self.lstm(x)
            attn_out, _ = self.mha(x, x, x, attn_mask=frag_mask)
            x = self.norm(x + attn_out)
            x = self.tr(x)
            return self.head(x[:, -1, :]).squeeze(-1)

    return FragmentHybrid()


def load_singularity_bundle(run_dir: Path, device: Optional[str] = None) -> Optional[dict[str, Any]]:
    """
    Load arch + weights from run_* folder. Returns dict with model, frag_mask, device_t, hp, seq_len, frag, run_dir.
    """
    run_dir = Path(run_dir)
    arch_path = run_dir / "arch.json"
    pt_path = run_dir / "model.pt"
    if not arch_path.exists() or not pt_path.exists():
        log.warning("Singularity bundle incomplete: %s (need arch.json + model.pt)", run_dir)
        return None

    try:
        import torch
    except ImportError:
        log.warning("PyTorch not installed — singularity inference disabled")
        return None

    arch = json.loads(arch_path.read_text(encoding="utf-8"))
    hp = arch["hyperparams"]
    seq_len = int(hp["seq_len"])
    frag = int(hp["fragment_len"])
    dropout = float(hp.get("dropout", 0.1))

    dev_s = device or ("cuda" if torch.cuda.is_available() else "cpu")
    device_t = torch.device(dev_s)

    model = _build_model(hp, dropout).to(device_t)
    frag_mask = build_fragment_mask(seq_len, frag, device_t)

    try:
        blob = torch.load(pt_path, map_location=device_t, weights_only=False)
    except TypeError:
        blob = torch.load(pt_path, map_location=device_t)

    if isinstance(blob, dict) and "state_dict" in blob:
        state = blob["state_dict"]
    elif isinstance(blob, dict) and any(str(k).startswith("proj.") for k in blob):
        state = blob
    else:
        log.warning("Unexpected checkpoint format in %s", pt_path)
        return None
    model.load_state_dict(state, strict=True)

    model.eval()
    return {
        "model": model,
        "frag_mask": frag_mask,
        "device": device_t,
        "hp": hp,
        "seq_len": seq_len,
        "frag": frag,
        "run_dir": run_dir,
        "architecture_id": arch.get("architecture_id", ""),
    }
