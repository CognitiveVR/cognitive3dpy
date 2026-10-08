"""Load a downloaded dataset with lerobot and sanity-check it."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from cognitive3dpy._lerobot.errors import C3DDatasetError
from cognitive3dpy._lerobot.writer import SIDECAR_DIR, quiet_lerobot


@dataclass
class ValidationReport:
    path: Path
    repo_id: str
    episodes: int
    frames: int
    fps: int
    features: dict[str, dict]
    nan_counts: dict[str, int] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


def validate_dataset(
    path: str | Path, repo_id: str | None = None, *, check_images: bool = True
) -> ValidationReport:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    root = Path(path)
    info_path = root / "meta" / "info.json"
    if not info_path.exists():
        raise C3DDatasetError(f"{root} is not a LeRobot dataset (no meta/info.json).")
    info = json.loads(info_path.read_text())
    if repo_id is None:
        sidecar = root / SIDECAR_DIR / "download.json"
        if sidecar.exists():
            repo_id = json.loads(sidecar.read_text()).get("repo_id")
    repo_id = repo_id or "local/dataset"

    with quiet_lerobot():
        from lerobot.datasets.lerobot_dataset import LeRobotDataset

        dataset = LeRobotDataset(repo_id, root=root)

    features = dict(dataset.meta.features)
    report = ValidationReport(
        path=root,
        repo_id=repo_id,
        episodes=dataset.meta.total_episodes,
        frames=dataset.meta.total_frames,
        fps=dataset.meta.fps,
        features=features,
    )
    if report.frames != info.get("total_frames"):
        report.problems.append(
            f"info.json says {info.get('total_frames')} frames, "
            f"the loader sees {report.frames}"
        )
    if len(dataset) != report.frames:
        report.problems.append(
            f"dataset length {len(dataset)} != total_frames {report.frames}"
        )

    table = pq.read_table(sorted((root / "data").rglob("*.parquet")))
    episode_index = table.column("episode_index").to_numpy()
    timestamps = table.column("timestamp").to_numpy()
    for episode in np.unique(episode_index):
        ts = timestamps[episode_index == episode]
        if len(ts) > 1 and not np.all(np.diff(ts) > 0):
            report.problems.append(
                f"episode {episode}: timestamps are not strictly increasing"
            )

    for name, spec in features.items():
        if spec["dtype"] in ("float32", "float64") and name in table.column_names:
            column = table.column(name)
            if tuple(spec["shape"]) != (1,):
                column = pc.list_flatten(column)
            values = np.asarray(column.to_numpy(zero_copy_only=False), dtype=float)
            report.nan_counts[name] = int(np.isnan(values).sum())

    if check_images and len(dataset) > 0:
        for i in sorted({0, len(dataset) - 1}):
            sample = dataset[i]
            for name, spec in features.items():
                if spec["dtype"] not in ("image", "video"):
                    continue
                shape = tuple(sample[name].shape)
                height, width, channels = spec["shape"]
                if shape not in ((channels, height, width), (height, width, channels)):
                    report.problems.append(
                        f"{name} frame {i} has shape {shape}, "
                        f"expected {tuple(spec['shape'])}"
                    )
    return report
