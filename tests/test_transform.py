"""Tests for _transform.py."""

import logging

import polars as pl
import pytest

from cognitive3dpy._transform import (
    _clean_name,
    coerce_types,
    join_scene_names,
    normalize_columns,
    select_compact,
    to_output,
)

# --- _clean_name ---


def test_clean_name_dots_to_underscores():
    assert _clean_name("c3d.app.name") == "c3d_app_name"


def test_clean_name_camel_case():
    assert _clean_name("endDate") == "end_date"
    assert _clean_name("sessionId") == "session_id"


def test_clean_name_spaces_and_hyphens():
    assert _clean_name("my custom-prop") == "my_custom_prop"


def test_clean_name_percent_and_hash():
    assert _clean_name("success%") == "success_percent"
    assert _clean_name("#items") == "_numberitems"


def test_clean_name_collapses_underscores():
    assert _clean_name("a..b") == "a_b"


def test_clean_name_lowercase():
    assert _clean_name("MyField") == "my_field"


# --- normalize_columns ---


def test_normalize_columns_unnests_properties():
    df = pl.DataFrame(
        {
            "sessionId": ["s1"],
            "properties": [{"c3d.app.name": "MyApp"}],
        }
    )
    df = df.with_columns(
        pl.col("properties").cast(pl.Struct({"c3d.app.name": pl.Utf8}))
    )
    result = normalize_columns(df)
    assert "c3d_app_name" in result.columns
    assert "properties" not in result.columns


def test_normalize_columns_renames_date():
    df = pl.DataFrame({"date": ["2025-01-01"], "sessionId": ["s1"]})
    result = normalize_columns(df)
    assert "session_date" in result.columns
    assert "date" not in result.columns


def test_normalize_columns_renames_camel_case():
    df = pl.DataFrame({"sessionId": ["s1"], "endDate": ["2025-01-01"]})
    result = normalize_columns(df)
    assert "session_id" in result.columns
    assert "end_date" in result.columns


# --- coerce_types ---


def test_coerce_types_duration_ms_to_s():
    df = pl.DataFrame({"duration": [1800000]})
    result = coerce_types(df)
    assert "duration_s" in result.columns
    assert "duration" not in result.columns
    assert result["duration_s"][0] == pytest.approx(1800.0)


def test_coerce_types_parses_session_date():
    df = pl.DataFrame({"session_date": ["2025-06-01T10:00:00Z"]})
    result = coerce_types(df)
    assert result.schema["session_date"] == pl.Datetime("us", "UTC")


def test_coerce_types_tags_list_to_string():
    df = pl.DataFrame({"tags": [["a", "b"]]})
    result = coerce_types(df)
    assert result["tags"][0] == "a, b"


def test_coerce_types_non_metric_int_unchanged():
    # project_id and scene_version_id are identifiers and must stay Int64.
    df = pl.DataFrame(
        {
            "project_id": pl.Series([42], dtype=pl.Int64),
            "scene_version_id": pl.Series([7], dtype=pl.Int64),
        }
    )
    result = coerce_types(df)
    assert result.schema["project_id"] == pl.Int64
    assert result.schema["scene_version_id"] == pl.Int64




def test_coerce_types_casts_null_columns_to_string():
    df = pl.DataFrame({"session_id": ["abc"], "empty_col": [None]})
    assert df.schema["empty_col"] == pl.Null
    result = coerce_types(df)
    assert result.schema["empty_col"] == pl.Utf8


# --- join_scene_names ---


def test_join_scene_names_maps_lookup():
    df = pl.DataFrame({"scene_version_id": [11, 10]})
    lookup = {"11": "Training Room", "10": "Training Room v1"}
    result = join_scene_names(df, lookup)
    assert result["scene_name"].to_list() == ["Training Room", "Training Room v1"]


def test_join_scene_names_null_when_no_lookup():
    df = pl.DataFrame({"scene_version_id": [11]})
    result = join_scene_names(df, {})
    assert result["scene_name"][0] is None


def test_join_scene_names_warns_on_unmapped_id(caplog):
    df = pl.DataFrame({"scene_version_id": ["111", "999", "111"]})
    lookup = {"111": "Main Scene"}
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._transform"):
        result = join_scene_names(df, lookup)
    assert result["scene_name"].null_count() == 1
    assert any("unrecognised scene_version_id" in m for m in caplog.messages)


# --- select_compact ---


def test_select_compact_keeps_known_columns():
    df = pl.DataFrame({"session_id": ["s1"], "unknown_col": [1]})
    result = select_compact(df)
    assert "session_id" in result.columns
    assert "unknown_col" not in result.columns


# --- to_output ---


def test_to_output_polars():
    df = pl.DataFrame({"a": [1]})
    result = to_output(df, "polars")
    assert isinstance(result, pl.DataFrame)


def test_to_output_pandas():
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    df = pl.DataFrame({"a": [1]})
    result = to_output(df, "pandas")
    assert isinstance(result, pd.DataFrame)


def test_normalize_columns_duplicate_property_dropped(caplog):
    df = pl.DataFrame(
        {
            "c3d_participant_oculus_username": ["top_level"],
            "properties": [{"c3d_participant_oculus_username": "from_props"}],
        }
    )
    df = df.with_columns(
        pl.col("properties").cast(
            pl.Struct({"c3d_participant_oculus_username": pl.Utf8})
        )
    )
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._transform"):
        result = normalize_columns(df)
    assert "c3d_participant_oculus_username" in result.columns
    assert result["c3d_participant_oculus_username"][0] == "top_level"
    assert "properties" not in result.columns
    assert any("Dropping duplicate property field" in m for m in caplog.messages)


def test_normalize_columns_clean_name_collision_dropped(caplog):
    df = pl.DataFrame({"c3d.foo.bar": ["a"], "c3d_foo_bar": ["b"]})
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._transform"):
        result = normalize_columns(df)
    assert "c3d_foo_bar" in result.columns
    assert result.shape[1] == 1
    assert any("cleans to" in m for m in caplog.messages)


def test_normalize_columns_no_duplicates_unchanged():
    df = pl.DataFrame(
        {
            "sessionId": ["s1"],
            "properties": [{"c3d.app.name": "MyApp"}],
        }
    )
    df = df.with_columns(
        pl.col("properties").cast(pl.Struct({"c3d.app.name": pl.Utf8}))
    )
    result = normalize_columns(df)
    assert "session_id" in result.columns
    assert "c3d_app_name" in result.columns
    assert "properties" not in result.columns


def test_to_output_invalid():
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="output must be"):
        to_output(df, "csv")


# --- Duplicate column handling (DS-561) ---


def test_normalize_columns_triple_duplicate_property_dropped(caplog):
    """Three property fields that already exist as top-level columns."""
    df = pl.DataFrame(
        {
            "fieldA": ["top_a"],
            "fieldB": ["top_b"],
            "fieldC": ["top_c"],
            "properties": [
                {"fieldA": "prop_a", "fieldB": "prop_b", "fieldC": "prop_c"}
            ],
        }
    )
    df = df.with_columns(
        pl.col("properties").cast(
            pl.Struct({"fieldA": pl.Utf8, "fieldB": pl.Utf8, "fieldC": pl.Utf8})
        )
    )
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._transform"):
        result = normalize_columns(df)
    assert result["field_a"][0] == "top_a"
    assert result["field_b"][0] == "top_b"
    assert result["field_c"][0] == "top_c"
    assert "properties" not in result.columns
    assert sum("Dropping duplicate property field" in m for m in caplog.messages) == 3


def test_normalize_columns_realistic_project_4460_collision(caplog):
    """Realistic scenario: c3d.participant.oculus_username (property) collides
    with c3d_participant_oculus_username (top-level) after cleaning."""
    df = pl.DataFrame(
        {
            "c3d_participant_oculus_username": ["top_level_value"],
            "c3d.participant.oculus_username": ["dot_notation_value"],
        }
    )
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._transform"):
        result = normalize_columns(df)
    # First occurrence (c3d_participant_oculus_username) kept
    assert result.shape[1] == 1
    assert "c3d_participant_oculus_username" in result.columns
    assert result["c3d_participant_oculus_username"][0] == "top_level_value"
    assert any("which already exists" in m for m in caplog.messages)


def test_normalize_columns_dedup_with_compact():
    """Dedup should not break compact column selection."""
    df = pl.DataFrame(
        {
            "sessionId": ["s1"],
            "c3d_participant_oculus_username": ["top_level"],
            "c3d.participant.oculus_username": ["dot_notation"],
            "properties": [{"c3d.app.name": "MyApp"}],
        }
    )
    df = df.with_columns(
        pl.col("properties").cast(pl.Struct({"c3d.app.name": pl.Utf8}))
    )
    result = normalize_columns(df)
    compacted = select_compact(result)
    # Should not raise — compact selection should work on deduped frame
    assert "c3d_app_name" in compacted.columns
    assert compacted.shape[0] == 1


def test_normalize_columns_dedup_with_coerce_types_overrides():
    """Property overrides should apply to the surviving column after dedup."""
    df = pl.DataFrame(
        {
            "c3d.geo.latitude": [42],
            "c3d_geo_latitude": [43],
        }
    )
    result = normalize_columns(df)
    # First column (c3d.geo.latitude) kept, cleaned to c3d_geo_latitude
    assert result.shape[1] == 1
    assert result["c3d_geo_latitude"][0] == 42

    overrides = {"c3d_geo_latitude": pl.Float64}
    result = coerce_types(result, property_overrides=overrides)
    assert result.schema["c3d_geo_latitude"] == pl.Float64
    assert result["c3d_geo_latitude"][0] == pytest.approx(42.0)
