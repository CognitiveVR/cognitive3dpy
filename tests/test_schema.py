"""Tests for the schema registry and unified coerce_types()."""

import logging

import polars as pl
import pytest

from cognitive3dpy._schema import (
    EVENT_SCHEMA,
    EXITPOLL_SCHEMA,
    SESSION_OBJECTIVE_SCHEMA,
    SESSION_PROPERTY_OVERRIDES,
    SESSION_RAW_OVERRIDES,
    SESSION_SCHEMA,
    empty_frame,
)
from cognitive3dpy._schema_generated import (
    EVENT_FIELD_TYPES,
    SESSION_FIELD_TYPES,
    SESSION_PROPERTY_TYPES,
)
from cognitive3dpy._transform import coerce_types

# --- Generated schema dicts ---


class TestGeneratedDicts:
    def test_session_field_types_not_empty(self):
        assert len(SESSION_FIELD_TYPES) > 0

    def test_session_field_types_contains_key_fields(self):
        assert "date" in SESSION_FIELD_TYPES
        assert "duration" in SESSION_FIELD_TYPES
        assert "sessionId" in SESSION_FIELD_TYPES

    def test_session_property_types_not_empty(self):
        assert len(SESSION_PROPERTY_TYPES) > 0

    def test_session_property_types_contains_metrics(self):
        assert "c3d.metrics.fps_score" in SESSION_PROPERTY_TYPES
        assert SESSION_PROPERTY_TYPES["c3d.metrics.fps_score"] == pl.Float64

    def test_session_property_types_overrides_applied(self):
        """Fields with known misclassifications should be corrected."""
        assert SESSION_PROPERTY_TYPES["c3d.participant.height"] == pl.Float64
        assert SESSION_PROPERTY_TYPES["c3d.participant.armlength"] == pl.Float64
        assert SESSION_PROPERTY_TYPES["c3d.participant.Age"] == pl.Float64
        assert SESSION_PROPERTY_TYPES["c3d.multiplayer.port"] == pl.Int64

    def test_event_field_types_not_empty(self):
        assert len(EVENT_FIELD_TYPES) > 0

    def test_event_field_types_contains_coordinates(self):
        assert EVENT_FIELD_TYPES["xCoord"] == pl.Float64
        assert EVENT_FIELD_TYPES["yCoord"] == pl.Float64
        assert EVENT_FIELD_TYPES["zCoord"] == pl.Float64


# --- SESSION_RAW_OVERRIDES ---


class TestSessionRawOverrides:
    def test_hmd_is_utf8(self):
        assert SESSION_RAW_OVERRIDES["hmd"] == pl.Utf8

    def test_mixed_hmd_types_coerced(self):
        """schema_overrides prevents inference failure on mixed hmd types."""
        data = [
            {"sessionId": "a", "hmd": None},
            {"sessionId": "b", "hmd": 1},
            {"sessionId": "c", "hmd": "Oculus Quest 3"},
        ]
        df = pl.DataFrame(data, schema_overrides={"hmd": pl.Utf8})
        assert df.schema["hmd"] == pl.Utf8
        assert df["hmd"].to_list() == [None, "1", "Oculus Quest 3"]

    def test_includes_generated_field_types(self):
        assert "date" in SESSION_RAW_OVERRIDES
        assert "duration" in SESSION_RAW_OVERRIDES
        assert "sessionId" in SESSION_RAW_OVERRIDES

    def test_includes_manual_supplements(self):
        assert "endDate" in SESSION_RAW_OVERRIDES
        assert "friendlyName" in SESSION_RAW_OVERRIDES
        assert "tags" in SESSION_RAW_OVERRIDES


# --- SESSION_PROPERTY_OVERRIDES ---


class TestSessionPropertyOverrides:
    def test_names_are_normalized(self):
        """Property override keys should be snake_case (post-normalization)."""
        assert "c3d_app_name" in SESSION_PROPERTY_OVERRIDES
        assert "c3d_metrics_fps_score" in SESSION_PROPERTY_OVERRIDES

    def test_no_dots_in_keys(self):
        for key in SESSION_PROPERTY_OVERRIDES:
            assert "." not in key, f"Key {key!r} still has dots"


# --- empty_frame ---


class TestEmptyFrame:
    @pytest.mark.parametrize("domain", ["session", "event", "objective", "exitpoll"])
    def test_zero_rows(self, domain):
        df = empty_frame(domain)
        assert len(df) == 0

    @pytest.mark.parametrize(
        "domain,expected_schema",
        [
            ("session", SESSION_SCHEMA),
            ("event", EVENT_SCHEMA),
            ("objective", SESSION_OBJECTIVE_SCHEMA),
            ("exitpoll", EXITPOLL_SCHEMA),
        ],
    )
    def test_correct_schema(self, domain, expected_schema):
        df = empty_frame(domain)
        assert df.schema == expected_schema

    def test_invalid_domain_raises(self):
        with pytest.raises(ValueError, match="Unknown domain"):
            empty_frame("bogus")


# --- Unified coerce_types ---


class TestCoerceTypesDateParsing:
    def test_session_date_parsed(self):
        df = pl.DataFrame({"session_date": ["2025-06-01T10:00:00Z"]})
        result = coerce_types(df)
        assert result.schema["session_date"] == pl.Datetime("us", "UTC")

    def test_end_date_parsed(self):
        df = pl.DataFrame({"end_date": ["2025-06-01T10:30:00Z"]})
        result = coerce_types(df)
        assert result.schema["end_date"] == pl.Datetime("us", "UTC")

    def test_event_date_parsed(self):
        df = pl.DataFrame({"event_date": ["2025-06-01T10:05:00Z"]})
        result = coerce_types(df)
        assert result.schema["event_date"] == pl.Datetime("us", "UTC")

    def test_malformed_date_becomes_null(self):
        df = pl.DataFrame({
            "session_date": ["2025-06-01T10:00:00Z", "not-a-date"],
            "value": [1, 2],
        })
        result = coerce_types(df)
        assert result["session_date"][0] is not None
        assert result["session_date"][1] is None
        # Rest of row preserved
        assert result["value"].to_list() == [1, 2]

    def test_malformed_date_logs_warning(self, caplog):
        df = pl.DataFrame({"session_date": ["not-a-date"]})
        with caplog.at_level(logging.WARNING):
            coerce_types(df)
        assert "could not be parsed" in caplog.text

    def test_impossible_date_before_2015_logs_warning(self, caplog):
        df = pl.DataFrame({"session_date": ["2005-01-01T00:00:00Z"]})
        with caplog.at_level(logging.WARNING):
            coerce_types(df)
        assert "before 2015" in caplog.text

    def test_missing_timezone_logs_warning(self, caplog):
        df = pl.DataFrame({"session_date": ["2025-06-01T10:00:00"]})
        with caplog.at_level(logging.WARNING):
            coerce_types(df)
        assert "lack a timezone suffix" in caplog.text


class TestCoerceTypesStepTimestamp:
    def test_utf8_step_timestamp_parsed(self):
        df = pl.DataFrame({"step_timestamp": ["2025-06-15T14:30:00Z"]})
        result = coerce_types(df)
        assert result.schema["step_timestamp"] == pl.Datetime("us", "UTC")

    def test_int64_step_timestamp_parsed(self):
        df = pl.DataFrame({"step_timestamp": [1718459400000]})
        result = coerce_types(df)
        assert result.schema["step_timestamp"] == pl.Datetime("us", "UTC")

    def test_float64_step_timestamp_parsed(self):
        df = pl.DataFrame({"step_timestamp": [1718459400000.0]})
        result = coerce_types(df)
        assert result.schema["step_timestamp"] == pl.Datetime("us", "UTC")


class TestCoerceTypesPropertyOverrides:
    def test_overrides_cast_columns(self):
        df = pl.DataFrame({
            "c3d_geo_latitude": [42],  # Int64 from API
            "c3d_app_name": ["MyApp"],
        })
        overrides = {
            "c3d_geo_latitude": pl.Float64,
            "c3d_app_name": pl.Utf8,
        }
        result = coerce_types(df, property_overrides=overrides)
        assert result.schema["c3d_geo_latitude"] == pl.Float64
        assert result["c3d_geo_latitude"][0] == 42.0

    def test_overrides_skip_missing_columns(self):
        df = pl.DataFrame({"session_id": ["abc"]})
        overrides = {"nonexistent_col": pl.Float64}
        result = coerce_types(df, property_overrides=overrides)
        assert result.schema == df.schema

    def test_overrides_skip_matching_dtype(self):
        df = pl.DataFrame({"c3d_app_name": ["MyApp"]})
        overrides = {"c3d_app_name": pl.Utf8}  # already Utf8
        result = coerce_types(df, property_overrides=overrides)
        assert result.schema["c3d_app_name"] == pl.Utf8


class TestCoerceTypesExisting:
    """Verify existing behavior is preserved after refactor."""

    def test_duration_converted(self):
        df = pl.DataFrame({"duration": [249492]})
        result = coerce_types(df)
        assert "duration_s" in result.columns
        assert "duration" not in result.columns
        assert result["duration_s"][0] == pytest.approx(249.492)

    def test_tags_joined(self):
        df = pl.DataFrame({"tags": [["a", "b", "c"]]})
        result = coerce_types(df)
        assert result["tags"][0] == "a, b, c"

    def test_metric_columns_in_property_overrides(self):
        assert SESSION_PROPERTY_OVERRIDES["c3d_metrics_fps_score"] == pl.Float64
        key = "c3d_metric_components_comfort_score_head_orientation_score"
        assert SESSION_PROPERTY_OVERRIDES[key] == pl.Float64
        assert SESSION_PROPERTY_OVERRIDES["c3d_roomsize_meters"] == pl.Float64

    def test_null_columns_become_utf8(self):
        df = pl.DataFrame({"unknown_col": [None, None]})
        result = coerce_types(df)
        assert result.schema["unknown_col"] == pl.Utf8

    def test_empty_dataframe_returned_as_is(self):
        df = pl.DataFrame({"session_date": pl.Series([], dtype=pl.Utf8)})
        result = coerce_types(df)
        assert result.is_empty()
