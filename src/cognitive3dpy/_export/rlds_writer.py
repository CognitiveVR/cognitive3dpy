"""Episodes as an RLDS dataset, the Open X-Embodiment format: TFDS TFRecord
shards plus ``features.json`` and ``dataset_info.json``, written without
TensorFlow. ``tfds.builder_from_directory(<path>)`` loads the folder.

Each session is one episode whose ``steps`` hold ``observation`` (the primary
camera as ``image``, other cameras as ``<camera>_image``, ``state`` and every
other feature under its flat name), ``action`` (the next step's state),
``reward``, ``discount``, ``is_first``/``is_last``/``is_terminal`` and
``language_instruction``.
"""

from __future__ import annotations

import io
import json
import re
from pathlib import Path

import numpy as np
from PIL import Image

from cognitive3dpy._export import tfrecord
from cognitive3dpy._export.errors import C3DDatasetError
from cognitive3dpy._export.mapping import (
    STATE_KEY,
    CacheFor,
    DatasetLayout,
    EpisodePlan,
    camera_feature_key,
    next_state_actions,
    obs_name,
)
from cognitive3dpy._export.progress import progress_bar
from cognitive3dpy._export.sidecar import staged, write_sidecars

VERSION = "1.0.0"
SHARD_BYTES = 256 * 2**20
_CLASS = "tensorflow_datasets.core.features."
_TEMPLATE = "{DATASET}-{SPLIT}.{FILEFORMAT}-{SHARD_X_OF_Y}"


def builder_name(repo_id: str) -> str:
    """A TFDS dataset name: lower snake case, ``cognitive3d_<name>``."""
    name = re.sub(r"[^a-z0-9]+", "_", repo_id.split("/")[-1].lower()).strip("_")
    return f"cognitive3d_{name}" if name else "cognitive3d"


def _tensor(shape, dtype: str, description: str | None = None) -> dict:
    spec = {
        "pythonClassName": _CLASS
        + ("tensor_feature.Tensor" if shape else "scalar.Scalar"),
        "tensor": {
            "dtype": dtype,
            "encoding": "none",
            "shape": {"dimensions": [str(d) for d in shape]} if shape else {},
        },
    }
    if description:
        spec["description"] = description
    return spec


def _image(shape, encoding: str, description: str) -> dict:
    return {
        "pythonClassName": _CLASS + "image_feature.Image",
        "image": {
            "dtype": "uint8",
            "encodingFormat": encoding,
            "shape": {"dimensions": [str(d) for d in shape]},
        },
        "description": description,
    }


def _text(description: str | None = None) -> dict:
    spec = {"pythonClassName": _CLASS + "text_feature.Text", "text": {}}
    if description:
        spec["description"] = description
    return spec


def _dict(features: dict) -> dict:
    return {
        "pythonClassName": _CLASS + "features_dict.FeaturesDict",
        "featuresDict": {"features": features},
    }


def observation_names(layout: DatasetLayout) -> dict[str, str]:
    """Feature key -> observation name; the clock camera (or the first) is
    ``image``."""
    names = {key: obs_name(key) for key in layout.features}
    if layout.cameras:
        kind, arg = layout.clock
        primary = arg if kind == "camera" and arg in layout.cameras else None
        primary = primary or next(iter(layout.cameras))
        names[camera_feature_key(primary)] = "image"
    return names


def _encodings(plans: list[EpisodePlan], layout: DatasetLayout) -> dict[str, str]:
    out = {}
    for camera_id in layout.cameras:
        key = camera_feature_key(camera_id)
        formats = {r.extension for p in plans for r in p.images.get(key, [])}
        out[key] = "jpeg" if formats <= {"jpg"} else "png"
    return out


def features_json(
    layout: DatasetLayout, encodings: dict[str, str], names: dict[str, str]
) -> dict:
    observation = {}
    for key, spec in layout.features.items():
        description = ", ".join(str(n) for n in spec.get("names") or [])
        if spec["dtype"] == "image":
            observation[names[key]] = _image(spec["shape"], encodings[key], key)
        else:
            observation[names[key]] = _tensor(spec["shape"], spec["dtype"], description)
    state = layout.features[STATE_KEY]
    steps = {
        "observation": _dict(observation),
        "action": _tensor(
            state["shape"],
            "float32",
            "next step's state: " + ", ".join(state["names"]),
        ),
        "reward": _tensor((), "float32"),
        "discount": _tensor((), "float32"),
        "is_first": _tensor((), "bool"),
        "is_last": _tensor((), "bool"),
        "is_terminal": _tensor((), "bool"),
        "language_instruction": _text(),
    }
    metadata = {
        "file_path": _text("<project_id>/<session_id>"),
        "session_id": _text(),
        "project_id": _tensor((), "int64"),
        "start_time_ms": _tensor((), "int64", "session start, epoch milliseconds"),
    }
    return _dict(
        {
            "steps": {
                "pythonClassName": _CLASS + "dataset_feature.Dataset",
                "sequence": {"feature": _dict(steps), "length": "-1"},
            },
            "episode_metadata": _dict(metadata),
        }
    )


class _FrameEncoder:
    """Frames as their recorded bytes when the encoding and size already match,
    re-encoded otherwise."""

    def __init__(self, cache_for: CacheFor, layout: DatasetLayout, encodings):
        self._cache_for = cache_for
        self._shapes = {
            camera_feature_key(c): shape for c, shape in layout.cameras.items()
        }
        self._encodings = encodings
        self._last: dict[str, tuple] = {}

    def encode(self, session, feature: str, ref) -> bytes:
        last = self._last.get(feature)
        if last is not None and last[0] == ref:
            return last[1]
        data = self._cache_for(session).camera.read(ref)
        height, width = self._shapes[feature]
        encoding = self._encodings[feature]
        with Image.open(io.BytesIO(data)) as image:
            same_format = (image.format or "").lower() == encoding
            if not (
                same_format and image.size == (width, height) and image.mode == "RGB"
            ):
                rgb = image.convert("RGB")
                if rgb.size != (width, height):
                    rgb = rgb.resize((width, height))
                buffer = io.BytesIO()
                rgb.save(buffer, format=encoding.upper(), quality=95)
                data = buffer.getvalue()
        self._last[feature] = (ref, data)
        return data


def episode_example(
    plan: EpisodePlan,
    layout: DatasetLayout,
    names: dict[str, str],
    encoder: _FrameEncoder,
    task: str,
    bar=None,
) -> bytes:
    n = len(plan)
    features = {}
    for key, values in plan.numeric.items():
        name = f"steps/observation/{names[key]}"
        if layout.features[key]["dtype"] == "int64":
            features[name] = tfrecord.int64_feature(values)
        else:
            features[name] = tfrecord.float_feature(values)
    for feature, refs in plan.images.items():
        frames = []
        for ref in refs:
            frames.append(encoder.encode(plan.session, feature, ref))
            if bar is not None:
                bar.update(1)
        features[f"steps/observation/{names[feature]}"] = tfrecord.bytes_feature(frames)
    first = np.zeros(n, dtype=np.int64)
    first[:1] = 1
    last = np.zeros(n, dtype=np.int64)
    last[-1:] = 1
    features["steps/action"] = tfrecord.float_feature(
        next_state_actions(plan.numeric[STATE_KEY])
    )
    features["steps/reward"] = tfrecord.float_feature(np.zeros(n))
    features["steps/discount"] = tfrecord.float_feature(np.ones(n))
    features["steps/is_first"] = tfrecord.int64_feature(first)
    features["steps/is_last"] = tfrecord.int64_feature(last)
    features["steps/is_terminal"] = tfrecord.int64_feature(last)
    features["steps/language_instruction"] = tfrecord.bytes_feature([task.encode()] * n)
    session = plan.session
    start_ms = int(round(session.start_timestamp * 1000))
    features["episode_metadata/file_path"] = tfrecord.bytes_feature(
        [f"{session.project_id}/{session.session_id}".encode()]
    )
    features["episode_metadata/session_id"] = tfrecord.bytes_feature(
        [session.session_id.encode()]
    )
    features["episode_metadata/project_id"] = tfrecord.int64_feature(
        [session.project_id]
    )
    features["episode_metadata/start_time_ms"] = tfrecord.int64_feature([start_ms])
    return tfrecord.example(features)


def write_rlds(
    root: Path,
    name: str,
    layout: DatasetLayout,
    plans: list[EpisodePlan],
    cache_for: CacheFor,
    task: str,
    manifest: dict,
    *,
    coord: str,
    progress: bool = True,
) -> None:
    """Writes the TFDS folder ``root`` (via ``<root>.partial``)."""
    encodings = _encodings(plans, layout)
    names = observation_names(layout)
    encoder = _FrameEncoder(cache_for, layout, encodings)
    with staged(root) as partial:
        shards: list[tuple[Path, int, int]] = []  # (temp path, episodes, bytes)
        writer = None
        for index, plan in enumerate(plans):
            images = sum(len(refs) for refs in plan.images.values())
            desc = f"Writing episode {index} ({plan.session.session_id})"
            with progress_bar(
                total=images or len(plan), desc=desc, unit="frame", disable=not progress
            ) as bar:
                record = episode_example(plan, layout, names, encoder, task, bar)
                if not plan.images:
                    bar.update(len(plan))
            if writer is None or writer.bytes >= SHARD_BYTES:
                if writer is not None:
                    writer.close()
                path = partial / f".shard-{len(shards):05d}"
                writer = tfrecord.TFRecordWriter(path)
                shards.append((path, 0, 0))
            try:
                writer.write(record)
            except ValueError as e:
                raise C3DDatasetError(
                    f"Episode {index} ({plan.session.session_id}) is too large for "
                    f"one TFRecord ({e}); lower the frame rate (fps=, "
                    "clock='fixed:<hz>') or leave the camera out of streams."
                ) from e
            path, episodes, _ = shards[-1]
            shards[-1] = (path, episodes + 1, writer.bytes)
        if writer is not None:
            writer.close()

        for i, (path, _, _) in enumerate(shards):
            path.rename(partial / f"{name}-train.tfrecord-{i:05d}-of-{len(shards):05d}")
        (partial / "features.json").write_text(
            json.dumps(features_json(layout, encodings, names), indent=4)
        )
        sessions = ", ".join(p.session.session_id for p in plans)
        info = {
            "citation": "",
            "description": (
                f"Cognitive3D sessions ({sessions}) as RLDS episodes at "
                f"{layout.fps} fps; coordinates: {coord}."
            ),
            "fileFormat": "tfrecord",
            "location": {"urls": ["https://cognitive3d.com"]},
            "moduleName": "cognitive3dpy",
            "name": name,
            "releaseNotes": {VERSION: "Exported by cognitive3dpy."},
            "splits": [
                {
                    "filepathTemplate": _TEMPLATE,
                    "name": "train",
                    "numBytes": str(sum(size for _, _, size in shards)),
                    "shardLengths": [str(count) for _, count, _ in shards],
                }
            ],
            "version": VERSION,
        }
        (partial / "dataset_info.json").write_text(json.dumps(info, indent=2))
        manifest["rlds"] = {
            "builder": name,
            "version": VERSION,
            "observation_names": {k: v for k, v in names.items()},
            "action": "the next step's observation.state (the last step keeps its own)",
        }
        write_sidecars(
            partial, plans, layout, cache_for, coord, manifest, progress=progress
        )
