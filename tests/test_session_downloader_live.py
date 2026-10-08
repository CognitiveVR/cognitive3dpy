"""Live download of a real session. Opt in with:

    C3D_LIVE_TESTS=1 C3D_API_KEY=orgkey-… C3D_BASE_URL=https://api.c3ddev.com \\
        uv run pytest tests/test_session_downloader_live.py -s
"""

import importlib.util
import io
import os

import numpy as np
import pytest
from PIL import Image

import cognitive3dpy as c3d
from cognitive3dpy import session_downloader

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("C3D_LIVE_TESTS") != "1", reason="set C3D_LIVE_TESTS=1"
    ),
    pytest.mark.skipif(
        importlib.util.find_spec("lerobot") is None, reason="needs the lerobot extra"
    ),
]

PROJECT_ID = int(os.environ.get("C3D_TEST_PROJECT", "700"))
SESSION_ID = os.environ.get("C3D_TEST_SESSION", "1790083950_tb4-sim-fedora")


@pytest.fixture
def live_api(monkeypatch):
    # conftest clears C3D_BASE_URL and installs a test key; restore the real ones
    monkeypatch.setenv(
        "C3D_BASE_URL", os.environ.get("C3D_TEST_BASE_URL", "https://api.c3ddev.com")
    )
    c3d.c3d_auth()


def test_live_download(live_api, tmp_path):
    from cognitive3dpy._client import get_base_url
    from cognitive3dpy._lerobot.http import HttpClient
    from cognitive3dpy._lerobot.sessions import fetch_session

    summary = session_downloader.inspect(SESSION_ID, project_id=PROJECT_ID)
    camera_id, camera = next(iter(summary["cameras"].items()))

    result = session_downloader.download(
        output=tmp_path / "leRobot",
        project_id=PROJECT_ID,
        session_id=SESSION_ID,
        cache_dir=tmp_path / "cache",
    )
    print(f"\ncamera transports used: {sorted(result.camera.transports)}")
    assert result.frames == camera["frames"] and result.camera.from_cache == 0

    ds = result.load()
    with HttpClient(os.environ["C3D_API_KEY"], get_base_url()) as http:
        session = fetch_session(http, PROJECT_ID, SESSION_ID, progress=False)
        refs = session.cameras[camera_id]
        for i in np.linspace(0, len(refs) - 1, 5).astype(int):
            ref, sample = refs[i], ds[int(i)]
            j = np.searchsorted(session.gaze.times, ref.time, side="right") - 1
            p = session.gaze.positions[max(int(j), 0)]
            state = np.asarray(sample["observation.state"])
            assert np.allclose(state, [-p[0], p[1], p[2]], atol=1e-5)
            jpeg = http.request_bytes(
                "GET",
                f"/v0/versions/{ref.version_id}/sessions/{SESSION_ID}/cameraFrames/"
                f"{camera_id}/{ref.date}/{ref.seq}",
                params={"projectId": PROJECT_ID},
                context="spot check",
            )
            pixels = np.asarray(Image.open(io.BytesIO(jpeg)).convert("RGB")) / 255.0
            image = np.asarray(sample[f"observation.images.{camera_id}"])
            assert np.allclose(image.transpose(1, 2, 0), pixels, atol=1e-6)
