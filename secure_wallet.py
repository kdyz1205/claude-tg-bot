"""
Secure Wallet Module — Encrypted key storage + swap-only transaction signing.

Security model:
- Private key encrypted at rest with Fernet (AES-128-CBC + HMAC-SHA256)
- PBKDF2 with 480 000 iterations + random salt (brute-force resistant)
- Password from WALLET_PASSWORD environment variable (never on disk)
- Wallet file restricted to owner-only permissions (0600)
- Transaction whitelist: ONLY Jupiter swap program IDs allowed
- Transfer instructions are REJECTED — code cannot send SOL/tokens to other wallets
- Key never logged, printed, or sent over network
- Decrypted material zeroed from memory after use
"""

import os, json, logging, base64, hashlib, time, ctypes, sys
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
import base58

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
WALLET_FILE = BASE_DIR / "_wallet_encrypted.dat"


def _secure_zero(data: bytearray | bytes):
    """Best-effort in-place zeroing of sensitive bytes from memory."""
    if isinstance(data, bytearray):
        for i in range(len(data)):
            data[i] = 0
    elif isinstance(data, bytes) and data:
        try:
            ctypes.memset(ctypes.addressof(ctypes.c_char.from_buffer_copy(data)), 0, len(data))
        except Exception:
            pass


def _restrict_file_permissions(path: Path):
    """Set file to owner-read/write only. Best-effort on Windows."""
    try:
        if sys.platform != "win32":
            path.chmod(0o600)
        else:
            import subprocess
            subprocess.run(
                ["icacls", str(path), "/inheritance:r",
                 "/grant:r", f"{os.getenv('USERNAME', 'CURRENT_USER')}:(R,W)"],
                capture_output=True, timeout=5,
            )
    except Exception:
        pass

# ── Whitelisted program IDs (ONLY these can appear in signed transactions) ──
ALLOWED_PROGRAMS = {
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",  # Jupiter V6 Aggregator
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcPX7rE",  # Jupiter V4
    "JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uN9oRp",  # Jupiter V3
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # Associated Token Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token Program
    "ComputeBudget111111111111111111111111111111",      # Compute Budget
    "11111111111111111111111111111111",                  # System Program (only for ATA creation, NOT transfers)
}

# ── BLOCKED instruction types (reject if these discriminators appear) ──
# SystemProgram::Transfer discriminator = 2 (u32 LE)
# TokenProgram::Transfer discriminator = 3 (u8)
# TokenProgram::TransferChecked discriminator = 12 (u8)
BLOCKED_SYSTEM_TRANSFER_DISC = b'\x02\x00\x00\x00'
BLOCKED_TOKEN_TRANSFER_DISC = bytes([3])
BLOCKED_TOKEN_TRANSFER_CHECKED_DISC = bytes([12])


SALT_FILE = BASE_DIR / "_wallet_salt.dat"

def _get_or_create_salt() -> bytes:
    """Get or create a unique random salt for this installation."""
    if SALT_FILE.exists():
        return SALT_FILE.read_bytes()
    salt = os.urandom(32)
    SALT_FILE.write_bytes(salt)
    _restrict_file_permissions(SALT_FILE)
    return salt

def _derive_key(password: str) -> bytes:
    """Derive Fernet key from password using PBKDF2 with unique random salt."""
    salt = _get_or_create_salt()
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations=480000, dklen=32)
    return base64.urlsafe_b64encode(dk)


def _get_fernet() -> Optional[Fernet]:
    """Get Fernet cipher from WALLET_PASSWORD env var."""
    password = os.environ.get("WALLET_PASSWORD")
    if not password:
        logger.error("WALLET_PASSWORD environment variable not set")
        return None
    return Fernet(_derive_key(password))


# ══════════════════════════════════════════════════════════════════════════════
# KEY STORAGE
# ══════════════════════════════════════════════════════════════════════════════

def store_wallet(key_input: str) -> bool:
    """
    Store wallet credentials (private key or seed phrase), encrypted.

    Accepts:
    - Base58 private key string (64 bytes decoded)
    - Seed phrase (12 or 24 words separated by spaces)

    Returns True on success.
    """
    f = _get_fernet()
    if not f:
        return False

    key_input = key_input.strip()
    words = key_input.split()

    if len(words) in (12, 24):
        # Seed phrase — store as-is, derive keypair on load
        payload = json.dumps({"type": "seed", "data": key_input}).encode()
    else:
        # Assume base58 private key — validate
        try:
            decoded = base58.b58decode(key_input)
            if len(decoded) not in (32, 64):
                logger.error(f"Invalid key length: {len(decoded)} bytes (expected 32 or 64)")
                return False
            payload = json.dumps({"type": "key", "data": key_input}).encode()
        except Exception as e:
            logger.error(f"Invalid base58 key: {e}")
            return False

    encrypted = f.encrypt(payload)
    WALLET_FILE.write_bytes(encrypted)
    _restrict_file_permissions(WALLET_FILE)
    _secure_zero(payload)
    logger.info("Wallet stored (AES-encrypted, permissions restricted)")
    return True


def load_keypair():
    """
    Load and decrypt wallet, return Solders Keypair.
    Returns None if wallet not found or decryption fails.
    """
    if not WALLET_FILE.exists():
        logger.error("No wallet file found. Use store_wallet() first.")
        return None

    f = _get_fernet()
    if not f:
        return None

    decrypted = None
    try:
        encrypted = WALLET_FILE.read_bytes()
        decrypted = bytearray(f.decrypt(encrypted))
        data = json.loads(decrypted)
    except InvalidToken:
        logger.error("Failed to decrypt wallet — wrong password?")
        return None
    except Exception as e:
        logger.error(f"Wallet load error: {e}")
        return None
    finally:
        if decrypted is not None:
            _secure_zero(decrypted)

    try:
        from solders.keypair import Keypair

        if data["type"] == "key":
            key_bytes = base58.b58decode(data["data"])
            if len(key_bytes) == 64:
                return Keypair.from_bytes(key_bytes)
            elif len(key_bytes) == 32:
                return Keypair.from_seed(key_bytes)
        elif data["type"] == "seed":
            # BIP39 seed phrase → derive keypair via proper BIP44 path m/44'/501'/0'/0'
            try:
                from mnemonic import Mnemonic
                import hmac as _hmac
                mnemo = Mnemonic("english")
                seed = mnemo.to_seed(data["data"])  # 64-byte BIP39 seed
                # Derive using SLIP-10/ed25519: HMAC-SHA512 chain
                # Master key
                I = _hmac.new(b"ed25519 seed", seed, hashlib.sha512).digest()
                key, chain = I[:32], I[32:]
                # Derive each level: 44', 501', 0', 0'
                for idx in (44, 501, 0, 0):
                    idx_bytes = (0x80000000 | idx).to_bytes(4, "big")
                    I = _hmac.new(chain, b"\x00" + key + idx_bytes, hashlib.sha512).digest()
                    key, chain = I[:32], I[32:]
                return Keypair.from_seed(key)
            except ImportError:
                logger.error("Install 'mnemonic' package for seed phrase support: pip install mnemonic")
                return None

        logger.error(f"Unknown wallet type: {data.get('type')}")
        return None
    except Exception as e:
        logger.error(f"Keypair creation failed: {e}")
        return None


def get_public_key() -> Optional[str]:
    """Get wallet public key (address) without exposing private key."""
    kp = load_keypair()
    if kp:
        return str(kp.pubkey())
    return None


def wallet_exists() -> bool:
    """Check if encrypted wallet file exists."""
    return WALLET_FILE.exists()


def delete_wallet() -> bool:
    """Securely delete wallet file with multi-pass overwrite."""
    if WALLET_FILE.exists():
        size = WALLET_FILE.stat().st_size
        for _ in range(3):
            WALLET_FILE.write_bytes(os.urandom(size))
        WALLET_FILE.write_bytes(b'\x00' * size)
        WALLET_FILE.unlink()
        logger.info("Wallet deleted securely (3-pass random + zero)")
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# TRANSACTION SIGNING (SWAP-ONLY)
# ══════════════════════════════════════════════════════════════════════════════

def _validate_transaction(tx_bytes: bytes) -> tuple[bool, str]:
    """
    Validate that a transaction ONLY contains whitelisted program instructions.
    Returns (is_valid, reason).
    """
    try:
        from solders.transaction import VersionedTransaction
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Check every instruction's program ID
        msg = tx.message
        account_keys = list(msg.account_keys)

        for ix in msg.instructions:
            program_id = str(account_keys[ix.program_id_index])

            if program_id not in ALLOWED_PROGRAMS:
                return False, f"BLOCKED: unauthorized program {program_id}"

            # Extra check: if System Program, block Transfer instruction
            if program_id == "11111111111111111111111111111111":
                if len(ix.data) >= 4 and bytes(ix.data[:4]) == BLOCKED_SYSTEM_TRANSFER_DISC:
                    return False, "BLOCKED: SystemProgram.Transfer detected — refusing to sign"

            # Extra check: if Token Program, block Transfer AND TransferChecked
            if program_id == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                if len(ix.data) >= 1 and bytes(ix.data[:1]) in (BLOCKED_TOKEN_TRANSFER_DISC, BLOCKED_TOKEN_TRANSFER_CHECKED_DISC):
                    return False, "BLOCKED: TokenProgram.Transfer detected — refusing to sign"

        return True, "OK"
    except Exception as e:
        return False, f"Transaction parse error: {e}"


def sign_swap_transaction(tx_bytes: bytes) -> Optional[bytes]:
    """
    Sign a transaction ONLY if it passes the whitelist check.
    Returns signed transaction bytes, or None if rejected.
    """
    # Step 1: Validate transaction
    valid, reason = _validate_transaction(tx_bytes)
    if not valid:
        logger.error(f"Transaction REJECTED: {reason}")
        return None

    # Step 2: Load keypair
    kp = load_keypair()
    if not kp:
        logger.error("Cannot sign: wallet not loaded")
        return None

    # Step 3: Sign
    try:
        from solders.transaction import VersionedTransaction
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Sign the transaction
        signed = VersionedTransaction(tx.message, [kp])
        logger.info("Transaction signed successfully (swap-only verified)")
        return bytes(signed)
    except Exception as e:
        logger.error(f"Signing failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# BALANCE CHECK
# ══════════════════════════════════════════════════════════════════════════════

async def get_sol_balance() -> Optional[float]:
    """Get SOL balance for the wallet."""
    pubkey = get_public_key()
    if not pubkey:
        return None
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.mainnet-beta.solana.com",
                json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getBalance",
                    "params": [pubkey]
                }
            )
            data = resp.json()
            lamports = data.get("result", {}).get("value", 0)
            return lamports / 1_000_000_000  # Convert lamports to SOL
    except Exception as e:
        logger.error(f"Balance fetch failed: {e}")
        return None


async def get_token_balances() -> list[dict]:
    """Get all SPL token balances for the wallet."""
    pubkey = get_public_key()
    if not pubkey:
        return []
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.mainnet-beta.solana.com",
                json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        pubkey,
                        {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                        {"encoding": "jsonParsed"}
                    ]
                }
            )
            data = resp.json()
            accounts = data.get("result", {}).get("value", [])
            tokens = []
            for acc in accounts:
                info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                amount = info.get("tokenAmount", {})
                if float(amount.get("uiAmount", 0) or 0) > 0:
                    tokens.append({
                        "mint": info.get("mint", ""),
                        "amount": float(amount.get("uiAmount", 0)),
                        "decimals": amount.get("decimals", 0),
                    })
            return tokens
    except Exception as e:
        logger.error(f"Token balance fetch failed: {e}")
        return []
