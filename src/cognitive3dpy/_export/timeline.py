"""Aligning streams onto one frame grid."""

from __future__ import annotations

import numpy as np


def derive_fps(times: np.ndarray, fallback: int = 10) -> int:
    if len(times) < 2:
        return fallback
    span = float(times[-1] - times[0])
    if span <= 0:
        return fallback
    return max(1, round((len(times) - 1) / span))


def hold_indices(source_times: np.ndarray, query_times: np.ndarray) -> np.ndarray:
    """Index of the latest source sample at or before each query; -1 if none."""
    return np.searchsorted(source_times, query_times, side="right") - 1


def sample_and_hold(
    source_times: np.ndarray,
    source_values: np.ndarray,
    query_times: np.ndarray,
    *,
    before_first: str = "nan",
) -> np.ndarray:
    """Step-hold resampling. ``before_first="nan"`` leaves a gap before the first
    sample, ``"clamp"`` repeats the first sample."""
    if before_first not in ("nan", "clamp"):
        raise ValueError("before_first must be 'nan' or 'clamp'")
    values = np.asarray(source_values, dtype=np.float64)
    out = np.full((len(query_times),) + values.shape[1:], np.nan, dtype=np.float64)
    if len(source_times) == 0:
        return out
    idx = hold_indices(np.asarray(source_times), np.asarray(query_times))
    valid = idx >= 0
    out[valid] = values[idx[valid]]
    if before_first == "clamp":
        out[~valid] = values[0]
    return out


def event_index_per_frame(
    event_times: np.ndarray, event_codes: np.ndarray, frame_times: np.ndarray
) -> np.ndarray:
    """Code of the latest event in (t[i-1], t[i]] for each frame i (frame 0 takes
    everything up to t[0]); -1 when there is none."""
    out = np.full(len(frame_times), -1, dtype=np.int64)
    if len(event_times) == 0 or len(frame_times) == 0:
        return out
    upper = np.searchsorted(event_times, frame_times, side="right")
    lower = np.concatenate(([0], upper[:-1]))
    has_event = upper > lower
    out[has_event] = event_codes[upper[has_event] - 1]
    return out


def parse_clock(clock: str | None) -> tuple[str, str | float | None]:
    """``"camera:<id>"``, ``"camera"``, ``"gaze"``, ``"fixed:<hz>"`` or None (auto)."""
    if clock is None or clock == "auto":
        return "auto", None
    if clock in ("gaze", "camera"):
        return clock, None
    if clock.startswith("camera:") and len(clock) > len("camera:"):
        return "camera", clock.split(":", 1)[1]
    if clock.startswith("fixed:"):
        try:
            hz = float(clock.split(":", 1)[1])
        except ValueError:
            hz = 0.0
        if hz <= 0:
            raise ValueError(
                f"Invalid clock {clock!r}: the fixed rate must be positive, "
                "e.g. 'fixed:10'"
            )
        return "fixed", hz
    raise ValueError(f"Invalid clock {clock!r}; use camera[:<id>], gaze or fixed:<hz>")
