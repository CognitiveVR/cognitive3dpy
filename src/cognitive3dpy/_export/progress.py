"""Progress bars styled like huggingface_hub's."""

from __future__ import annotations

from tqdm.auto import tqdm


def progress_bar(
    *,
    total: int | None = None,
    desc: str = "",
    unit: str = "it",
    disable: bool = False,
    leave: bool = True,
    initial: int = 0,
) -> tqdm:
    """``initial`` counts work already done (e.g. cached), so it is not timed."""
    is_bytes = unit == "B"
    return tqdm(
        total=total,
        initial=initial,
        desc=desc,
        unit=unit,
        unit_scale=is_bytes,
        unit_divisor=1024 if is_bytes else 1000,
        disable=disable,
        leave=leave,
        dynamic_ncols=True,
    )


def format_bytes(n: float) -> str:
    for suffix in ("B", "kB", "MB"):
        if abs(n) < 1024:
            return f"{n:.0f}{suffix}" if suffix == "B" else f"{n:.1f}{suffix}"
        n /= 1024
    return f"{n:.1f}GB"
