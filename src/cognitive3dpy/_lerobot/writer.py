"""Write episode plans with lerobot's own dataset writer."""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
import warnings
from pathlib import Path

from cognitive3dpy._lerobot.mapping import (
    CacheFor,
    DatasetLayout,
    EpisodePlan,
    FrameLoader,
)
from cognitive3dpy._lerobot.progress import progress_bar

SIDECAR_DIR = "c3d"


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
    partial = root.with_name(root.name + ".partial")
    if partial.exists():
        shutil.rmtree(partial)
    partial.parent.mkdir(parents=True, exist_ok=True)

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
                desc = f"Writing episode {episode_index} ({plan.session.session_id})"
                with progress_bar(
                    total=len(plan), desc=desc, unit="frame", disable=not progress
                ) as bar:
                    for i in range(len(plan)):
                        frame = {key: values[i] for key, values in plan.numeric.items()}
                        for feature, refs in plan.images.items():
                            frame[feature] = loader.load(plan.session, feature, refs[i])
                        frame["task"] = task
                        dataset.add_frame(frame)
                        bar.update(1)
                dataset.save_episode()
        finally:
            dataset.finalize()

    from cognitive3dpy._lerobot.sidecar import safe_name, write_session_sidecar

    sessions_dir = partial / SIDECAR_DIR / "sessions"
    for episode, plan in zip(sidecar.get("episodes", []), plans):
        session_dir = sessions_dir / safe_name(plan.session.session_id)
        files = write_session_sidecar(
            session_dir,
            plan,
            layout,
            cache_for(plan.session),
            coord,
            progress=progress,
        )
        episode["sidecar"] = files
    sessions_dir.mkdir(parents=True, exist_ok=True)
    (partial / SIDECAR_DIR / "download.json").write_text(json.dumps(sidecar, indent=2))

    if root.exists():
        shutil.rmtree(root)
    partial.rename(root)
