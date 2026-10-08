# cognitive3dpy

`cognitive3dpy` is a Python client for the [Cognitive3D](https://cognitive3d.com) analytics API. It turns raw JSON responses into a Polars or Panadas DataFrames ready for analysis, handling authentication, pagination, property flattening, name resolution, and type coercion so you can go from API key to analysis-ready data in a few lines of code.

## Installation

```bash
pip install cognitive3dpy
```

To enable pandas output (`output="pandas"`), install with the optional extra:

```bash
pip install "cognitive3dpy[pandas]"
```

## Getting an API Key

1. Log in to your [Cognitive3D Dashboard](https://app.cognitive3d.com)
2. Click your profile icon in the top-right corner and select **Settings**
3. Navigate to the **API Keys** tab
4. Click **Create API Key**, give it a name, and copy the generated key

Store the key in a `.env` file (never commit this to version control):

```bash
C3D_API_KEY=your_api_key_here
```

Or set it as an environment variable in your shell:

```bash
export C3D_API_KEY=your_api_key_here
```

## Quick Start

```python
import cognitive3dpy as c3d

# Authenticate and set project
c3d.c3d_auth()        # reads C3D_API_KEY env var, or pass key directly
c3d.c3d_project(4460) # set default project ID

# Pull data
sessions         = c3d.c3d_sessions(n=100)
events           = c3d.c3d_events()
results          = c3d.c3d_objective_results(group_by="steps")
session_steps    = c3d.c3d_session_objectives()
polls            = c3d.c3d_exitpoll()
```

## Data Streams

| Function | Description | Key output columns |
|---|---|---|
| `c3d_sessions()` | Session-level metrics and properties | `session_id`, `duration_s`, `hmd`, `c3d_metrics_*`, `c3d_device_*`, `c3d_geo_*` |
| `c3d_sessions(session_type="scene")` | Sessions split by scene visited — one row per session-scene | Adds `scene_id`, `scene_version_id`, `scene_name` |
| `c3d_events()` | One row per in-session event with session context | `event_name`, `event_date`, `position_x/y/z`, `object`, `scene_name`, `prop_*` |
| `c3d_objective_results()` | Objective success/failure counts by version | `objective_name`, `version_number`, `succeeded`, `failed`, `completion_rate` |
| `c3d_objective_results(group_by="steps")` | Step-level detail for each objective | Adds `step_name`, `step_type`, `step_detail`, `avg_completion_time_s` |
| `c3d_session_objectives()` | Per-session objective step results — one row per step per session | `session_id`, `participant_id`, `objective_name`, `step_number`, `step_description`, `step_result`, `step_duration_sec` |
| `c3d_exitpoll()` | Exit poll survey responses | `question_title`, `value`, `value_label`, per hook/version |

## Sessions

Retrieve session-level data with optional date filtering and compact/full column modes.

```python
# Last 30 days, compact columns (default)
sessions = c3d.c3d_sessions(n=100)

# Custom date range, all columns
sessions = c3d.c3d_sessions(
    n=50,
    start_date="2025-01-01",
    end_date="2025-06-01",
    compact=False,
)
```

### Scene Sessions

Use `session_type="scene"` to get session data broken out by scene — one row per session-scene combination. This is useful for comparing metrics across scenes within the same session. Defaults to the latest version of each scene.

```python
# All scenes, latest versions (default)
scene_sessions = c3d.c3d_sessions(session_type="scene")

# Filter to a specific scene
scene_sessions = c3d.c3d_sessions(
    session_type="scene",
    scene_id="de704574-b03f-424e-be87-4985f85ed2e8",
)

# Filter to a specific scene version
scene_sessions = c3d.c3d_sessions(
    session_type="scene",
    scene_version_id=7011,
)
```

## Events

Retrieve per-event data with session context attached. Events are unnested from sessions — one row per event. Dynamic object IDs are resolved to friendly names, and scene version IDs are resolved to scene names.

```python
events = c3d.c3d_events(
    start_date="2025-01-01",
    n=20,
)
```

## Objective Results

Query objective success/failure counts, optionally sliced by version or with step-level detail.

```python
# By version (default)
results = c3d.c3d_objective_results(group_by="version")

# Step-level detail
detailed = c3d.c3d_objective_results(group_by="steps")
```

## Session Objectives

Retrieve per-session objective step results. Returns one row per step per session, with step descriptions and outcomes.

```python
session_steps = c3d.c3d_session_objectives(
    start_date="2025-01-01",
    end_date="2025-06-01",
)
```

## Exit Polls

Retrieve exit poll response counts across all hooks and versions. Returns one row per response option per question per version, with human-readable value labels.

```python
# All hooks and versions
polls = c3d.c3d_exitpoll()

# Filter to a specific hook and version
polls = c3d.c3d_exitpoll(hook="end_questions", version=3)
```

## LeRobot Datasets (Robotics)

`session_downloader` turns a session into a [LeRobot](https://github.com/huggingface/lerobot) dataset for robot learning: head pose, gaze, sensors, events, camera frames, lidar and the play-area boundary, aligned on one clock.

```bash
pip install "cognitive3dpy[lerobot]"   # Python 3.12+; cannot be combined with the pandas extra
```

```python
import cognitive3dpy as c3d
from cognitive3dpy import session_downloader

c3d.c3d_auth()  # reads C3D_API_KEY
session_downloader.download(output="datasets", project_id=700, session_id="1790083950_tb4-sim-fedora")
```

The dataset is written to `datasets/cognitive3d/<session_id>` (`output` defaults to `datasets`), checked by loading it with lerobot, and opens with `LeRobotDataset(repo_id, root=path)` or `result.load()`. Progress bars show each download step. To look at it:

```bash
lerobot-dataset-viz --repo-id cognitive3d/<session_id> --root datasets/cognitive3d/<session_id> --mode local --episode-index 0
```

`lerobot-dataset-viz` plots `observation.state` and the camera images; the other features are in the dataset but that viewer does not chart them.

| Feature | Shape | From |
|---|---|---|
| `observation.state` | (3,), or (7,) with `head_rotation=True` | head position (+ quaternion) |
| `observation.gaze` | (3,) | gaze point; NaN when the gaze hit an object |
| `observation.sensors` | (channels,) | sensor channels, named in the feature's `names` |
| `observation.event` | (1,) int64 | index into `event_names` in `c3d/download.json`, else -1 |
| `observation.images.<camera>` | (H, W, 3) | camera frames |
| `observation.camera.<camera>.pose` | (7,) | camera pose px, py, pz, qx, qy, qz, qw |
| `observation.lidar.<sensor>.ranges` | (beams,) | laserscan ranges in metres; NaN for no return or out of range |
| `observation.lidar.<sensor>.scan_index` | (1,) int64 | point cloud held at this step, a file in the sidecar (below); -1 before the first |
| `observation.lidar.<sensor>.pose` | (7,) | pose of the held lidar scan |
| `observation.boundary.pose` | (7,) | play-area boundary pose |
| `observation.boundary.shape_index` | (1,) int64 | boundary polygon in effect, an entry in `boundary.json`; -1 before the first |
| `observation.c3d_time_ms` | (1,) int64 | real capture time, epoch ms |

- **One session per episode** — pass a list of `session_id`s (with `repo_id="org/name"`) for a multi-episode dataset.
- **Clock** — frames follow the camera when the session has one, otherwise the gaze stream; `clock="fixed:10"` resamples onto an exact grid. Other streams are sample-and-held.
- **Coordinates** — Cognitive3D records left-handed, Y-up, metres; datasets are converted to right-handed, Y-up by mirroring X (positions `(-x, y, z)`, quaternions `(qx, -qy, -qz, qw)`), like the Cognitive3D replay viewer. `coord="native"` keeps the raw values.
- **Lidar and boundary in full** — a frame holds only the latest scan, so every scan is also kept in `c3d/sessions/<session_id>/`: `lidar/<sensor>/scans.npz` (all laserscans), one `.npz` per point cloud (`points`, `colors`, `pose`; listed in `index.json`), and `boundary.json` (poses and polygons). Points are sensor-local: `world = rotate(pose.q, point) + pose.p`; `c3d/download.json` gives each laserscan's beam angles and the formula for its points.
- **Streamed** — camera frames and lidar scans arrive as a few paged streams rather than one request each; deployments without the stream routes fall back to per-item requests (`camera_transport` / `lidar_transport`).
- **Resumable** — camera frames and lidar scans are cached in `~/.cache/cognitive3dpy` (or `C3D_CACHE_DIR`); an interrupted download resumes and re-runs reuse them.
- **Other options** — `streams`, `sensors`, `head_rotation`, `task`, `fps`, `repo_id`; see `help(session_downloader.download)`. `session_downloader.inspect(session_id)` lists what a session contains, and `session_downloader.validate(path)` re-checks a dataset.

## Configuration

### API host

Requests go to `https://api.cognitive3d.com`. To use another environment, set `C3D_BASE_URL` before the first API call:

```bash
export C3D_BASE_URL=https://api.c3ddev.com
```

### Timeout

The default request timeout is 30 seconds per API call. To increase it:

```python
import cognitive3dpy as c3d

c3d.c3d_set_timeout(60)  # 60 seconds
```

Or set the `C3D_TIMEOUT` environment variable before your session starts:

```bash
export C3D_TIMEOUT=60
```

`c3d_set_timeout()` takes effect immediately. The environment variable is read once on first import, so it must be set before importing the package.

## Key Features

- **Compact mode** — `c3d_sessions(compact=True)` (default) returns ~40 curated columns; `compact=False` returns everything
- **Scene sessions** — `session_type="scene"` queries the latest version of each scene by default, giving the full project picture split by scene
- **Automatic name resolution** — dynamic object SDK IDs are resolved to friendly names in events and objective steps; scene version IDs are resolved to scene names
- **Date defaults** — all functions default to the last 30 days when no date range is specified
- **Column naming** — top-level API fields use `snake_case`; Cognitive3D properties retain their `c3d_` prefix (e.g., `c3d_metrics_fps_score`)
- **Polars-native** — returns `polars.DataFrame` by default; pass `output="pandas"` for a pandas DataFrame
- **Session filtering** — `exclude_test`, `exclude_idle`, `min_duration` parameters across functions

## Common Options

All data-fetching functions support these session filters:

- `exclude_test` / `exclude_idle` — filter out test and junk sessions (default `True`)
- `start_date` / `end_date` — date range as a `date`/`datetime` object, epoch timestamp, or `"YYYY-MM-DD"` string
- `min_duration` — minimum session duration in seconds
- `project_id` — override the default set by `c3d_project()`
- `output` — `"polars"` (default) or `"pandas"`
- `warn_empty` — emit a `UserWarning` when 0 rows are returned (default `True`); set to `False` to suppress

## Contributing

```bash
git clone https://github.com/CognitiveVR/cognitive3dpy.git
cd cognitive3dpy
uv sync --all-groups --extra pandas  # installs package + dev + pandas dependencies
uv run pytest tests/  # run tests
```

Releases are automated via [python-semantic-release](https://python-semantic-release.readthedocs.io/) on push to `main`. Use [conventional commits](https://www.conventionalcommits.org/) (`fix:`, `feat:`, `feat!:`) to trigger a release.
