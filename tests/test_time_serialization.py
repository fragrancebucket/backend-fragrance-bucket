from datetime import datetime
from app.main import as_utc, utc_iso


def test_mongo_naive_timestamp_serializes_as_utc():
    assert utc_iso(datetime(2026, 10, 1, 12, 0, 0)).endswith("+00:00")


def test_mongo_naive_timestamp_can_compare_with_utc_now():
    assert as_utc(datetime(2026, 10, 1, 12, 0, 0)).tzinfo is not None
