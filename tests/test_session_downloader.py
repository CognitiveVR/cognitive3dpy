"""Tests for session_downloader.py: the public API, end to end against a fake API.

Download/validate tests need the lerobot extra (Python 3.12+) and skip without it.
"""

import importlib.util
import io
import json
from unittest.mock import patch

import numpy as np
import pytest
from lerobot_fakes import make_session
from PIL import Image

import cognitive3dpy as c3d
from cognitive3dpy import session_downloader

needs_lerobot = pytest.mark.skipif(
    importlib.util.find_spec("lerobot") is None,
    reason="needs the lerobot extra (Python 3.12+)",
)


def scalar(value):
    return np.asarray(value).reshape(-1)[0]


@pytest.fixture
def no_sleep():
    with patch("cognitive3dpy._lerobot.http.time.sleep"):
        yield


@pytest.fixture
def fake(edge):
    return edge.add(make_session())


def download(tmp_path, fake, **kwargs):
    kwargs.setdefault("output", tmp_path / "leRobot")
    kwargs.setdefault("cache_dir", tmp_path / "cache")
    kwargs.setdefault("progress", False)
    return session_downloader.download(session_id=fake.session_id, **kwargs)


def test_inspect(fake):
    summary = session_downloader.inspect(fake.session_id)
    assert summary["project_id"] == 1234
    assert summary["streams"] == ["camera", "events", "gaze", "head", "sensors"]
    assert summary["gaze"] == {"samples": 100, "rate_hz": 10.0}
    assert summary["cameras"]["front"]["frames"] == 20
    assert summary["cameras"]["front"]["rate_hz"] == 2.0
    assert summary["sensors"] == ["battery.percent", "odom.speed"]


def test_project_id_falls_back_to_c3d_project(fake):
    c3d.c3d_project(fake.project_id)
    assert session_downloader.inspect(fake.session_id)["session_id"] == fake.session_id
    with pytest.raises(TypeError, match="project_id must be an int"):
        session_downloader.inspect(fake.session_id, project_id="1234")


def test_needs_c3d_auth(fake):
    from cognitive3dpy.auth import _config

    _config.api_key = None
    with pytest.raises(ValueError, match="c3d_auth"):
        session_downloader.inspect(fake.session_id)


def test_install_hint_when_the_extra_is_missing(tmp_path, monkeypatch):
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a: None if name == "lerobot" else real(name, *a),
    )
    with pytest.raises(ImportError, match=r"cognitive3dpy\[lerobot\]"):
        session_downloader.download(session_id="s", output=tmp_path)


def test_base_url_override(monkeypatch):
    from cognitive3dpy._client import BASE_URL, get_base_url

    assert get_base_url() == BASE_URL
    monkeypatch.setenv("C3D_BASE_URL", "https://api.c3ddev.com/v0/")
    assert get_base_url() == "https://api.c3ddev.com"


def test_default_repo_id_is_the_session_id():
    from cognitive3dpy._lerobot.download import default_repo_id

    assert default_repo_id(["1790083950_tb4-sim"]) == "cognitive3d/1790083950_tb4-sim"
    assert default_repo_id(["a b/c"]) == "cognitive3d/a_b_c"
    assert default_repo_id(["_x"]) == "cognitive3d/s_x"
    with pytest.raises(ValueError, match="repo_id is required"):
        default_repo_id(["a", "b"])


@needs_lerobot
def test_default_output_is_datasets_cognitive3d_session_id(tmp_path, fake, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = session_downloader.download(
        session_id=fake.session_id, cache_dir=tmp_path / "cache", progress=False
    )
    expected = tmp_path / "datasets" / "cognitive3d" / fake.session_id
    assert result.path == expected.resolve()
    assert result.repo_id == f"cognitive3d/{fake.session_id}"
    assert (expected / "meta" / "info.json").exists()


@needs_lerobot
def test_download_matches_the_source(tmp_path, fake, edge):
    c3d.c3d_auth("test-api-key")
    result = download(tmp_path, fake, project_id=1234)
    expected_path = tmp_path / "leRobot" / "cognitive3d" / fake.session_id
    assert result.path == expected_path.resolve()
    assert result.episodes == 1 and result.frames == 20 and result.fps == 2
    assert result.camera.transports == {"stream"}

    ds = result.load()
    assert len(ds) == 20
    cam = fake.docs["CAMERA"]["cameraFrames"]["42"]
    for i in (0, 7, 19):
        sample, row = ds[i], cam[i]
        assert scalar(sample["observation.c3d_time_ms"]) == row["date"]
        pose = row["pose"]
        expected = [-pose[0], pose[1], pose[2], pose[3], -pose[4], -pose[5], pose[6]]
        got = np.asarray(sample["observation.camera.front.pose"])
        assert np.allclose(got, expected, atol=1e-5)
        jpeg = fake.frames[(42, "front", row["date"], row["seq"])]
        pixels = np.asarray(Image.open(io.BytesIO(jpeg)).convert("RGB")) / 255.0
        image = np.asarray(sample["observation.images.front"]).transpose(1, 2, 0)
        assert np.allclose(image, pixels, atol=1e-6)

    sidecar = json.loads((result.path / "c3d" / "download.json").read_text())
    assert sidecar["coord"]["target"] == "rh_yup"
    assert sidecar["camera_transports"] == ["stream"]
    events_file = result.path / "c3d" / "sessions" / fake.session_id / "events.json"
    assert len(json.loads(events_file.read_text())["events"]) == 3
    assert not result.path.with_name(result.path.name + ".partial").exists()

    report = session_downloader.validate(result.path)
    assert report.ok and report.frames == 20


@needs_lerobot
def test_rerun_overwrites_and_reuses_the_frame_cache(tmp_path, fake, edge):
    download(tmp_path, fake)
    camera_calls = edge.calls["stream"] + edge.calls["frame"]
    again = download(tmp_path, fake)
    assert edge.calls["stream"] + edge.calls["frame"] == camera_calls
    assert again.camera.from_cache == 20
    with pytest.raises(FileExistsError):
        download(tmp_path, fake, overwrite=False)


@needs_lerobot
def test_per_frame_fallback_with_an_auth_outage(tmp_path, fake, edge, no_sleep):
    edge.stream_available = False
    edge.auth_outage = 0
    edge.fail_first = {"frame": [503, 502]}
    result = download(tmp_path, fake)
    assert result.camera.transports == {"per-frame"}
    assert result.camera.via_per_frame == 20


@needs_lerobot
def test_several_sessions_become_episodes(tmp_path, edge):
    a = edge.add(make_session("a", duration=6.0))
    b = edge.add(make_session("b", duration=4.0))
    common = {"output": tmp_path / "o", "cache_dir": tmp_path / "c", "progress": False}
    with pytest.raises(ValueError, match="repo_id is required"):
        session_downloader.download(session_id=["a", "b"], **common)
    result = session_downloader.download(
        session_id=[a.session_id, b.session_id], repo_id="acme/pair", **common
    )
    assert result.episodes == 2 and result.frames == 12 + 8
    assert session_downloader.validate(result.path).episodes == 2


@needs_lerobot
def test_progress_output(tmp_path, fake, capsys):
    result = download(tmp_path, fake, progress=True)
    captured = capsys.readouterr()
    assert "front" in captured.err and "Writing episode 0" in captured.err
    assert f"✔ 20 frames (1 episode, 2 fps) → {result.path}" in captured.out


@needs_lerobot
def test_unknown_session(tmp_path, edge):
    with pytest.raises(c3d.C3DNotFoundError, match="not found or has no data yet"):
        session_downloader.download(
            session_id="missing", output=tmp_path, progress=False
        )


@needs_lerobot
def test_a_bad_key_is_an_auth_error(tmp_path, fake):
    c3d.c3d_auth("orgkey-wrong-key-0000")
    with pytest.raises(c3d.C3DAuthError, match="orgkey-…0000"):
        download(tmp_path, fake)


def test_inspect_lists_lidar_and_boundary(edge):
    fake = edge.add(make_session(lidar=True, boundary=True))
    summary = session_downloader.inspect(fake.session_id)
    assert summary["lidar"]["scan"] == {
        "kind": "laserscan",
        "scans": 30,
        "rate_hz": 3.0,
        "versions": [42],
        "range_count": 8,
    }
    assert summary["lidar"]["depth"]["kind"] == "pointcloud"
    assert summary["lidar"]["depth"]["points_max"] == 13
    assert summary["boundary"] == {"poses": 3, "shapes": 2}


@needs_lerobot
def test_download_with_lidar_and_boundary(tmp_path, edge):
    from lerobot_fakes import laserscan_ranges

    fake = edge.add(make_session(lidar=True, boundary=True))
    result = download(tmp_path, fake)
    assert result.lidar.scans == len(fake.lidar_blobs) == result.lidar.downloaded
    assert result.lidar.transports == {"stream"}
    assert edge.calls["lidar"] == 0
    for key in (
        "observation.lidar.scan.ranges",
        "observation.lidar.scan.pose",
        "observation.lidar.depth.scan_index",
        "observation.lidar.depth.pose",
        "observation.boundary.pose",
        "observation.boundary.shape_index",
    ):
        assert key in result.features

    ds = result.load()
    sample = ds[len(ds) - 1]
    ranges = np.asarray(sample["observation.lidar.scan.ranges"])
    last_frame = fake.docs["CAMERA"]["cameraFrames"]["42"][-1]["time"]
    scans = [
        r for r in fake.docs["LIDAR"]["lidarScans"]["42"] if r["kind"] == "laserscan"
    ]
    held = max(r["seq"] for r in scans if r["time"] <= last_frame)
    expected = laserscan_ranges(held)
    expected[[2, 5]] = np.nan
    assert np.array_equal(ranges, expected, equal_nan=True)
    assert scalar(sample["observation.lidar.depth.scan_index"]) == 8
    assert scalar(sample["observation.boundary.shape_index"]) == 1

    session_dir = result.path / "c3d" / "sessions" / fake.session_id
    assert (session_dir / "lidar" / "scan" / "scans.npz").exists()
    index = json.loads((session_dir / "lidar" / "depth" / "index.json").read_text())
    assert len(index["scans"]) == 9
    assert (session_dir / "lidar" / "depth" / index["scans"][8]["file"]).exists()
    assert (session_dir / "boundary.json").exists()

    sidecar = json.loads((result.path / "c3d" / "download.json").read_text())
    assert sidecar["lidar"]["scan"]["range_count"] == 8
    assert "rotate(pose.q" in sidecar["lidar"]["scan"]["point"]
    assert sidecar["boundary"] is True
    assert sidecar["lidar_transports"] == ["stream"]
    assert sidecar["episodes"][0]["sidecar"]["depth"]["scans"] == 9
    assert session_downloader.validate(result.path).ok


@needs_lerobot
def test_lidar_falls_back_per_scan_on_an_older_deployment(tmp_path, edge):
    fake = edge.add(make_session(lidar=True))
    edge.lidar_stream_available = False
    result = download(tmp_path, fake)
    assert result.lidar.transports == {"per-scan"}
    assert result.lidar.via_per_scan == len(fake.lidar_blobs)
    assert session_downloader.validate(result.path).ok
