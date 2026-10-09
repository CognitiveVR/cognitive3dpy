"""Write episode plans with lerobot's own dataset writer."""

from __future__ import annotations

import contextlib
import logging
import warnings
from pathlib import Path

from cognitive3dpy._export.mapping import (
    CacheFor,
    DatasetLayout,
    EpisodePlan,
    FrameLoader,
)
from cognitive3dpy._export.progress import progress_bar
from cognitive3dpy._export.sidecar import staged, write_sidecars


@contextlib.contextmanager
def quiet_lerobot():
    """Hide lerobot's import-time torchcodec/ffmpeg warning (it falls back to pyav,
    and image datasets never decode video) and the `datasets` "Map" bars."""
    names = ("lerobot.utils.import_utils", "lerobot", "datasets")
    loggers = [logging.getLogger(name) for name in names]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.ERROR)
    restore_bars = None
    try:
        import datasets

        if datasets.is_progress_bar_enabled():
            datasets.disable_progress_bars()
            restore_bars = datasets.enable_progress_bars
    except Exception:
        pass
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield
    finally:
        for logger, level in zip(loggers, levels):
            logger.setLevel(level)
        if restore_bars:
            restore_bars()


def write_dataset(
    root: Path,
    repo_id: str,
    layout: DatasetLayout,
    plans: list[EpisodePlan],
    cache_for: CacheFor,
    task: str,
    sidecar: dict,
    *,
    coord: str = "rh_yup",
    progress: bool = True,
    image_writer_threads: int = 4,
) -> None:
    """Writes into ``<root>.partial`` and renames on success, so an interrupted run
    never leaves a dataset that looks complete. ``sidecar`` becomes
    ``c3d/download.json``; its ``episodes`` entries line up with ``plans``."""
    with staged(root) as partial:
        partial.rmdir()  # lerobot creates the dataset root itself
        with quiet_lerobot():
            from lerobot.datasets.lerobot_dataset import LeRobotDataset

            dataset = LeRobotDataset.create(
                repo_id=repo_id,
                fps=layout.fps,
                features=layout.features,
                root=partial,
                robot_type="cognitive3d",
                use_videos=False,
                image_writer_threads=image_writer_threads if layout.cameras else 0,
            )
            loader = FrameLoader(cache_for, layout)
            try:
                for episode_index, plan in enumerate(plans):
                    desc = (
                        f"Writing episode {episode_index} ({plan.session.session_id})"
                    )
                    with progress_bar(
                        total=len(plan), desc=desc, unit="frame", disable=not progress
                    ) as bar:
                        for i in range(len(plan)):
                            frame = {k: values[i] for k, values in plan.numeric.items()}
                            for feature, refs in plan.images.items():
                                frame[feature] = loader.load(
                                    plan.session, feature, refs[i]
                                )
                            frame["task"] = task
                            dataset.add_frame(frame)
                            bar.update(1)
                    dataset.save_episode()
            finally:
                dataset.finalize()

        write_sidecars(
            partial, plans, layout, cache_for, coord, sidecar, progress=progress
        )
