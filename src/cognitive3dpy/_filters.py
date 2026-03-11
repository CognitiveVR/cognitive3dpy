"""Session filter builder for the Cognitive3D API."""

from __future__ import annotations

from datetime import UTC, date, datetime

from dateutil import parser as _dateutil_parser


def _parse_date(value: str | int | float | date | datetime) -> date | datetime:
    """Accept a date, datetime, epoch timestamp, or flexible date string.

    Supported inputs:
        - ``date`` or ``datetime`` objects — returned as-is.
        - ``int`` or ``float`` — interpreted as an epoch timestamp.
          Values >= 1e12 are treated as **milliseconds**, otherwise **seconds**.
        - ``str`` — parsed flexibly, e.g. ``"2025-01-15"``, ``"2025/01/15"``,
          ``"2025, jan 15"``, ``"jan 15, 2025"``, ``"01/15/2025"``, etc.
    """
    if isinstance(value, (date, datetime)):
        return value
    if isinstance(value, (int, float)):
        epoch_s = value / 1000 if value >= 1e12 else value
        return datetime.fromtimestamp(epoch_s, tz=UTC)
    if isinstance(value, str):
        try:
            return _dateutil_parser.parse(value)
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"Unable to parse date string: {value!r}") from exc
    raise TypeError(
        f"Expected str, int, float, date, or datetime, got {type(value).__name__}"
    )


def _to_epoch_ms(d: date | datetime) -> int:
    """Convert a date or datetime to epoch milliseconds (UTC)."""
    if isinstance(d, datetime):
        dt = d if d.tzinfo else d.replace(tzinfo=UTC)
    else:
        dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def build_filters(
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    exclude_test: bool = True,
    exclude_idle: bool = True,
    min_duration: int = 0,
) -> list[dict]:
    """Build the sessionFilters array for API POST requests.

    Parameters
    ----------
    start_date : str, int, float, date, or datetime, optional
        Start of date range. Accepts date/datetime objects, epoch
        timestamps (seconds or milliseconds, auto-detected), or flexible
        string formats such as ``"2025-01-15"``, ``"2025/01/15"``,
        ``"2025, jan 15"``, ``"jan 15, 2025"``, etc.
        If ``None``, no start date filter is applied.
        Naive ``datetime`` objects (no timezone info) are assumed to be UTC.
    end_date : str, int, float, date, or datetime, optional
        End of date range (inclusive). Same formats as *start_date*.
        If ``None``, no end date filter is applied.
        Naive ``datetime`` objects (no timezone info) are assumed to be UTC.
        A plain ``date`` value is converted to midnight (00:00:00 UTC) of
        that day, so sessions occurring later that day will not be included.
        To capture a full day, pass the following day as ``end_date`` or use
        a ``datetime`` with time ``23:59:59``.
    exclude_test : bool
        Exclude sessions tagged as test.
    exclude_idle : bool
        Exclude sessions tagged as junk/idle.
    min_duration : int
        Minimum session duration in seconds. Converted to milliseconds
        for the API. 0 means no filter.
    """
    filters: list[dict] = []

    # Date range — only added when explicitly provided
    if start_date is not None:
        filters.append(
            {
                "field": {"fieldName": "date", "fieldParent": "session"},
                "op": "gte",
                "value": _to_epoch_ms(_parse_date(start_date)),
            }
        )
    if end_date is not None:
        filters.append(
            {
                "field": {"fieldName": "date", "fieldParent": "session"},
                "op": "lte",
                "value": _to_epoch_ms(_parse_date(end_date)),
            }
        )

    # Exclude test sessions
    if exclude_test:
        filters.append(
            {
                "op": "eq",
                "field": {
                    "nestedFieldName": "booleanSessionProp",
                    "path": "c3d.session_tag.test",
                },
                "value": False,
            }
        )

    # Exclude idle/junk sessions
    if exclude_idle:
        filters.append(
            {
                "op": "eq",
                "field": {
                    "nestedFieldName": "booleanSessionProp",
                    "path": "c3d.session_tag.junk",
                },
                "value": False,
            }
        )

    # Minimum duration (input is seconds, API expects milliseconds)
    if min_duration > 0:
        filters.append(
            {
                "field": {"fieldName": "duration", "fieldParent": "session"},
                "op": "gte",
                "value": min_duration * 1000,
            }
        )

    return filters
