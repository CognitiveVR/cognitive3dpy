"""Read a downloaded dataset back and sanity-check it, per format."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from cognitive3dpy._export.errors import C3DDatasetError
from cognitive3dpy._export.sidecar import SIDECAR_DIR
from cognitive3dpy._export.writer import quiet_lerobot


@dataclass
class ValidationReport:
    path: Path
    repo_id: str
    episodes: int
    frames: int
    fps: int | None
    features: dict[str, dict]
    nan_counts: dict[str, int] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    format: str = "lerobot"

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


def read_manifest(path: str | Path) -> dict:
    """``c3d/download.json`` of an export, or {} when there is none."""
    manifest = Path(path) / SIDECAR_DIR / "download.json"
    return json.loads(manifest.read_text()) if manifest.exists() else {}


def validate_export(path: str | Path, repo_id: str | None = None) -> ValidationReport:
    """Validate any export, by the format its manifest names (lerobot if none)."""
    manifest = read_manifest(path)
    fmt = manifest.get("format", "lerobot")
    if fmt == "lerobot":
        return validate_dataset(path, repo_id)
    validator = {
        "hdf5": validate_hdf5,
        "rlds": validate_rlds,
        "mcap": validate_mcap,
        "json": validate_json,
    }[fmt]
    report = ValidationReport(
        path=Path(path),
        repo_id=repo_id or manifest.get("repo_id", ""),
        episodes=0,
        frames=0,
        fps=manifest.get("fps"),
        features={},
        format=fmt,
    )
    validator(Path(path), manifest, report)
    return report


def _check_lengths(report, where: str, lengths: dict[str, int], expected: int):
    for name, length in lengths.items():
        if length != expected:
            report.problems.append(f"{where}: {name} has {length} rows, not {expected}")


def validate_hdf5(path: Path, manifest: dict, report: ValidationReport) -> None:
    import h5py

    files = manifest.get("hdf5", {}).get("files") or []
    if not files:
        report.problems.append("the manifest lists no HDF5 files")
    for name in files:
        with h5py.File(path / name, "r") as f:
            if "data" in f:  # robomimic
                total = 0
                for demo_name, demo in f["data"].items():
                    n = int(demo.attrs["num_samples"])
                    lengths = {k: len(demo[k]) for k in ("actions", "states", "dones")}
                    lengths.update({f"obs/{k}": len(v) for k, v in demo["obs"].items()})
                    _check_lengths(report, f"{name} {demo_name}", lengths, n)
                    report.features = {
                        k: {"shape": v.shape[1:]} for k, v in demo["obs"].items()
                    }
                    report.episodes += 1
                    total += n
                report.frames += total
                if int(f["data"].attrs.get("total", -1)) != total:
                    report.problems.append(f"{name}: data.attrs['total'] != {total}")
            else:  # aloha
                n = len(f["action"])
                obs = f["observations"]
                lengths = {k: len(v) for k, v in obs.items() if k != "images"}
                lengths.update(
                    {f"images/{k}": len(v) for k, v in obs["images"].items()}
                )
                _check_lengths(report, name, lengths, n)
                report.features = {
                    k: {"shape": v.shape[1:]} for k, v in obs.items() if k != "images"
                }
                report.episodes += 1
                report.frames += n


def validate_rlds(path: Path, manifest: dict, report: ValidationReport) -> None:
    import io

    from PIL import Image

    from cognitive3dpy._export import tfrecord

    info = json.loads((path / "dataset_info.json").read_text())
    features = json.loads((path / "features.json").read_text())
    steps = features["featuresDict"]["features"]["steps"]["sequence"]["feature"]
    observation = steps["featuresDict"]["features"]["observation"]["featuresDict"][
        "features"
    ]
    split = info["splits"][0]
    shards = sorted(path.glob(f"{info['name']}-train.tfrecord-*"))
    if len(shards) != len(split["shardLengths"]):
        report.problems.append(
            f"dataset_info lists {len(split['shardLengths'])} shards, "
            f"found {len(shards)}"
        )
    for shard, expected in zip(shards, split["shardLengths"]):
        episodes = 0
        for record in tfrecord.read_records(shard):
            example = tfrecord.parse_example(record)
            n = len(example["steps/is_first"])
            for name, spec in observation.items():
                values = example.get(f"steps/observation/{name}")
                if values is None:
                    report.problems.append(f"episode {report.episodes}: no {name}")
                    continue
                if "image" in spec:
                    dims = [int(d) for d in spec["image"]["shape"]["dimensions"]]
                    _check_lengths(
                        report, f"episode {report.episodes}", {name: len(values)}, n
                    )
                    for data in (values[0], values[-1]) if values else ():
                        with Image.open(io.BytesIO(data)) as image:
                            if (image.height, image.width) != tuple(dims[:2]):
                                report.problems.append(
                                    f"{name}: frame of {image.size}, expected {dims}"
                                )
                else:
                    dims = [
                        int(d) for d in spec["tensor"]["shape"].get("dimensions", [])
                    ]
                    width = int(np.prod(dims)) if dims else 1
                    _check_lengths(
                        report,
                        f"episode {report.episodes}",
                        {name: len(values) // width},
                        n,
                    )
                report.features[name] = spec
            report.episodes += 1
            report.frames += n
            episodes += 1
        if episodes != int(expected):
            report.problems.append(
                f"{shard.name}: {episodes} episodes, expected {expected}"
            )


def validate_mcap(path: Path, manifest: dict, report: ValidationReport) -> None:
    from mcap.reader import make_reader
    from mcap_ros2.decoder import DecoderFactory

    for bag in manifest.get("bags", []):
        file = path / bag["file"]
        if not (file.parent / "metadata.yaml").exists():
            report.problems.append(f"{file.parent}: no rosbag2 metadata.yaml")
        with open(file, "rb") as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            summary = reader.get_summary()
            counts = summary.statistics.channel_message_counts
            by_topic = {c.topic: counts.get(i, 0) for i, c in summary.channels.items()}
            for topic, info in bag["topics"].items():
                if by_topic.get(topic) != info["messages"]:
                    report.problems.append(
                        f"{file.name} {topic}: {by_topic.get(topic)} messages, "
                        f"expected {info['messages']}"
                    )
            seen = set()
            for _, channel, _, _ in reader.iter_decoded_messages(log_time_order=False):
                seen.add(channel.topic)
                if seen >= set(by_topic):
                    break
        report.episodes += 1
        report.frames += bag["messages"]
        report.features.update(
            {t: {"type": i["type"]} for t, i in bag["topics"].items()}
        )


def validate_json(path: Path, manifest: dict, report: ValidationReport) -> None:
    from cognitive3dpy._export.sidecar import safe_name

    written = manifest.get("files", {})
    for session_id, files in written.items():
        base = path if len(written) == 1 else path / safe_name(session_id)
        report.episodes += 1
        for name in files:
            target = base / name
            if not target.exists():
                report.problems.append(f"{target} is missing")
            elif name.endswith(".json"):
                try:
                    json.loads(target.read_bytes())
                    report.frames += 1
                except ValueError as e:
                    report.problems.append(f"{target}: {e}")
