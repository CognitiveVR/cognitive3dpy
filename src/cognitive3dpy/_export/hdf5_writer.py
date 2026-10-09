"""Episodes as HDF5 demonstrations, in the robomimic / Isaac Lab Mimic layout or
the ALOHA / ACT one.

robomimic: one ``<name>.hdf5`` with ``data/demo_<i>/{obs/*, actions, states,
rewards, dones}``, ``data.attrs["env_args"]`` and ``mask/train``.
ALOHA: one ``episode_<i>.hdf5`` per session with ``observations/{qpos, qvel,
images/<camera>, ...}`` and ``action``.

The state is the head pose (``observation.state``) and the action the next step's
state. Every other feature is an extra observation under its flat name.
"""

from __future__ import annotations

import json
from pathlib import Path

import h5py
import numpy as np

from cognitive3dpy._export.mapping import (
    STATE_KEY,
    CacheFor,
    DatasetLayout,
    EpisodePlan,
    FrameLoader,
    next_state_actions,
    obs_name,
)
from cognitive3dpy._export.progress import progress_bar
from cognitive3dpy._export.sidecar import staged, write_sidecars

HDF5_LAYOUTS = ("robomimic", "aloha")
_IMAGE_COMPRESSION = {"compression": "gzip", "compression_opts": 4}


def _names(layout: DatasetLayout, key: str) -> list[str]:
    return [str(n) for n in layout.features.get(key, {}).get("names") or []]


def _write_numeric(group: h5py.Group, layout: DatasetLayout, plan: EpisodePlan):
    for key, values in plan.numeric.items():
        if key == STATE_KEY:
            continue
        dataset = group.create_dataset(obs_name(key), data=values)
        dataset.attrs["names"] = _names(layout, key)


def _write_images(
    group: h5py.Group,
    layout: DatasetLayout,
    plan: EpisodePlan,
    loader: FrameLoader,
    bar,
    name_for=obs_name,
) -> None:
    for feature, refs in plan.images.items():
        height, width, channels = layout.features[feature]["shape"]
        dataset = group.create_dataset(
            name_for(feature),
            shape=(len(refs), height, width, channels),
            dtype=np.uint8,
            chunks=(1, height, width, channels),
            **_IMAGE_COMPRESSION,
        )
        for i, ref in enumerate(refs):
            dataset[i] = loader.load(plan.session, feature, ref)
            bar.update(1)


def _velocity(states: np.ndarray, times: np.ndarray) -> np.ndarray:
    if len(states) < 2:
        return np.zeros_like(states)
    return np.gradient(states.astype(np.float64), times, axis=0).astype(np.float32)


def write_hdf5(
    root: Path,
    name: str,
    layout: DatasetLayout,
    plans: list[EpisodePlan],
    cache_for: CacheFor,
    task: str,
    manifest: dict,
    *,
    coord: str,
    hdf5_layout: str = "robomimic",
    progress: bool = True,
) -> list[Path]:
    """Writes ``root`` (via ``<root>.partial``); returns the HDF5 files."""
    if hdf5_layout not in HDF5_LAYOUTS:
        raise ValueError(
            f"hdf5_layout must be one of {HDF5_LAYOUTS}, got {hdf5_layout!r}"
        )
    loader = FrameLoader(cache_for, layout)
    with staged(root) as partial:
        if hdf5_layout == "robomimic":
            files = [partial / f"{name}.hdf5"]
            _write_robomimic(files[0], layout, plans, loader, task, coord, progress)
        else:
            files = [partial / f"episode_{i}.hdf5" for i in range(len(plans))]
            for index, (path, plan) in enumerate(zip(files, plans)):
                _write_aloha(path, index, layout, plan, loader, task, coord, progress)
        manifest["hdf5"] = {
            "layout": hdf5_layout,
            "files": [f.name for f in files],
            "state": _names(layout, STATE_KEY),
            "action": "the next step's observation.state (the last step keeps its own)",
        }
        write_sidecars(
            partial, plans, layout, cache_for, coord, manifest, progress=progress
        )
    return [root / f.name for f in files]


def _bar(plan: EpisodePlan, index: int, progress: bool):
    images = sum(len(refs) for refs in plan.images.values())
    desc = f"Writing episode {index} ({plan.session.session_id})"
    return progress_bar(
        total=images or len(plan), desc=desc, unit="frame", disable=not progress
    )


def _write_robomimic(path, layout, plans, loader, task, coord, progress) -> None:
    with h5py.File(path, "w") as f:
        data = f.create_group("data")
        total = 0
        for index, plan in enumerate(plans):
            demo = data.create_group(f"demo_{index}")
            n = len(plan)
            state = plan.numeric[STATE_KEY]
            demo.attrs["num_samples"] = n
            demo.attrs["session_id"] = plan.session.session_id
            demo.create_dataset("states", data=state)
            demo.create_dataset("actions", data=next_state_actions(state))
            demo.create_dataset("rewards", data=np.zeros(n, dtype=np.float32))
            dones = np.zeros(n, dtype=np.int64)
            dones[-1:] = 1
            demo.create_dataset("dones", data=dones)
            obs = demo.create_group("obs")
            obs.create_dataset("state", data=state).attrs["names"] = _names(
                layout, STATE_KEY
            )
            _write_numeric(obs, layout, plan)
            with _bar(plan, index, progress) as bar:
                _write_images(obs, layout, plan, loader, bar)
                if not plan.images:
                    bar.update(n)
            total += n
        data.attrs["total"] = total
        data.attrs["fps"] = layout.fps
        data.attrs["task"] = task
        data.attrs["coordinate_system"] = coord
        data.attrs["env_args"] = json.dumps(
            {
                "env_name": "cognitive3d",
                "type": 2,
                "env_kwargs": {"fps": layout.fps, "task": task},
            }
        )
        names = [f"demo_{i}".encode() for i in range(len(plans))]
        f.create_group("mask").create_dataset("train", data=np.array(names))


def _write_aloha(path, index, layout, plan, loader, task, coord, progress) -> None:
    state = plan.numeric[STATE_KEY]
    with h5py.File(path, "w") as f:
        f.attrs["sim"] = False
        f.attrs["compress"] = False
        f.attrs["fps"] = layout.fps
        f.attrs["task"] = task
        f.attrs["session_id"] = plan.session.session_id
        f.attrs["coordinate_system"] = coord
        obs = f.create_group("observations")
        obs.create_dataset("qpos", data=state).attrs["names"] = _names(
            layout, STATE_KEY
        )
        obs.create_dataset("qvel", data=_velocity(state, plan.times))
        _write_numeric(obs, layout, plan)
        images = obs.create_group("images")
        with _bar(plan, index, progress) as bar:
            _write_images(
                images,
                layout,
                plan,
                loader,
                bar,
                name_for=lambda key: key.split(".")[-1],
            )
            if not plan.images:
                bar.update(len(plan))
        f.create_dataset("action", data=next_state_actions(state))
