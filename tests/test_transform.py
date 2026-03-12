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


def test_coerce_types_metric_int_cast_to_float64():
    # API may return whole-number scores as integers; they must become Float64.
    df = pl.DataFrame(
        {
            "c3d_metrics_fps_score": pl.Series([100], dtype=pl.Int64),
            "c3d_metrics_presence_score": pl.Series([75], dtype=pl.Int64),
        }
    )
    result = coerce_types(df)
    assert result.schema["c3d_metrics_fps_score"] == pl.Float64
    assert result.schema["c3d_metrics_presence_score"] == pl.Float64
    assert result["c3d_metrics_fps_score"][0] == pytest.approx(100.0)


def test_coerce_types_metric_components_int_cast_to_float64():
    df = pl.DataFrame(
        {
            "c3d_metric_components_comfort_score_head_orientation_score": pl.Series(
                [80], dtype=pl.Int64
            ),
        }
    )
    result = coerce_types(df)
    assert (
        result.schema["c3d_metric_components_comfort_score_head_orientation_score"]
        == pl.Float64
    )


def test_coerce_types_roomsize_meters_int_cast_to_float64():
    df = pl.DataFrame({"c3d_roomsize_meters": pl.Series([9], dtype=pl.Int64)})
    result = coerce_types(df)
    assert result.schema["c3d_roomsize_meters"] == pl.Float64


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


def test_coerce_types_metric_already_float64_unchanged():
    df = pl.DataFrame(
        {"c3d_metrics_fps_score": pl.Series([99.5], dtype=pl.Float64)}
    )
    result = coerce_types(df)
    assert result.schema["c3d_metrics_fps_score"] == pl.Float64
    assert result["c3d_metrics_fps_score"][0] == pytest.approx(99.5)


def test_coerce_types_metric_pandas_dtype_is_float64():
    pytest.importorskip("pyarrow")
    pytest.importorskip("pandas")
    df = pl.DataFrame(
        {"c3d_metrics_fps_score": pl.Series([100], dtype=pl.Int64)}
    )
    result = coerce_types(df)
    pandas_df = result.to_pandas()
    assert str(pandas_df["c3d_metrics_fps_score"].dtype) == "float64"


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


def test_to_output_invalid():
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="output must be"):
        to_output(df, "csv")
