"""portfolio_snapshot.get_latest_cache is in-process only (fast path contract)."""

from trading import portfolio_snapshot


def test_get_latest_cache_has_age_sec():
    s = portfolio_snapshot.get_latest_cache()
    assert isinstance(s, dict)
    assert "age_sec" in s
    assert "wallet" in s and "okx" in s


def test_get_latest_cache_matches_get_snapshot_shape():
    a = portfolio_snapshot.get_latest_cache()
    b = portfolio_snapshot.get_snapshot()
    assert set(a.keys()) == set(b.keys())
