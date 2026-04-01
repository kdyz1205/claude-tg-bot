import pytest

from gateway.jarvis_semantic import classify_intent, extract_wallet_clone_address
from pipeline.wallet_clone_pipeline import clone_skill_filename, normalize_wallet


@pytest.mark.asyncio
async def test_classify_wallet_clone_track_and_decode():
    addr = "0x1234567890123456789012345678901234567890"
    row = await classify_intent(f"追踪并破解地址 {addr}", uid=1)
    assert row["intent"] == "WALLET_CLONE"
    assert row["extracted_address"] == addr.lower()


@pytest.mark.asyncio
async def test_wallet_clone_takes_priority_over_factor_forge():
    """地址克隆短语优先于因子关键词。"""
    addr = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
    row = await classify_intent(
        f"克隆高手地址 {addr} 写个RSI因子",
        uid=1,
    )
    assert row["intent"] == "WALLET_CLONE"
    assert row["extracted_address"] == addr.lower()


def test_extract_wallet_clone_address_none_without_hex():
    assert extract_wallet_clone_address("追踪并破解地址") is None


def test_normalize_wallet():
    assert normalize_wallet("0xabc") is None
    a = "0x" + "a" * 40
    assert normalize_wallet(a) == a.lower()


def test_clone_skill_filename_eight_hex():
    w = "0xABCDEF0123456789ABCDEF0123456789ABCDEF01"
    assert clone_skill_filename(w) == "sk_clone_0xabcdef01.py"
