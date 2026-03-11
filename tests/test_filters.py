"""Tests for _filters.py."""

from datetime import UTC, date, datetime

import pytest

from cognitive3dpy._filters import _parse_date, _to_epoch_ms, build_filters


def test_parse_date_string():
    d = _parse_date("2025-06-01")
    assert d.year == 2025 and d.month == 6 and d.day == 1


def test_parse_date_object():
    d = date(2025, 6, 1)
    assert _parse_date(d) is d


def test_parse_epoch_seconds():
    dt = _parse_date(1_748_736_000)  # < 1e12, treated as seconds
    assert isinstance(dt, datetime)


def test_parse_epoch_milliseconds():
    dt = _parse_date(1_748_736_000_000)  # >= 1e12, treated as ms
    assert isinstance(dt, datetime)


def test_parse_date_invalid_string():
    with pytest.raises(ValueError):
        _parse_date("not-a-date")


def test_to_epoch_ms_date():
    ms = _to_epoch_ms(date(2025, 1, 1))
    assert ms == int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1000)


def test_build_filters_empty_when_no_dates():
    filters = build_filters()
    # Only exclude_test and exclude_idle filters
    assert len(filters) == 2


def test_build_filters_adds_date_range():
    filters = build_filters(start_date="2025-01-01", end_date="2025-12-31")
    fields = [f.get("field", {}).get("fieldName") for f in filters]
    assert fields.count("date") == 2


def test_build_filters_start_only():
    filters = build_filters(start_date="2025-01-01")
    date_filters = [f for f in filters if f.get("field", {}).get("fieldName") == "date"]
    assert len(date_filters) == 1
    assert date_filters[0]["op"] == "gte"


def test_build_filters_exclude_test():
    filters = build_filters(exclude_test=True, exclude_idle=False)
    test_filter = next(
        f for f in filters if f.get("field", {}).get("path") == "c3d.session_tag.test"
    )
    assert test_filter["value"] is False


def test_build_filters_exclude_idle():
    filters = build_filters(exclude_test=False, exclude_idle=True)
    idle_filter = next(
        f for f in filters if f.get("field", {}).get("path") == "c3d.session_tag.junk"
    )
    assert idle_filter["value"] is False


def test_build_filters_min_duration():
    filters = build_filters(exclude_test=False, exclude_idle=False, min_duration=60)
    dur_filter = next(
        f for f in filters if f.get("field", {}).get("fieldName") == "duration"
    )
    assert dur_filter["value"] == 60_000  # converted to ms


def test_build_filters_no_duration_when_zero():
    filters = build_filters(exclude_test=False, exclude_idle=False, min_duration=0)
    assert not any(f.get("field", {}).get("fieldName") == "duration" for f in filters)
