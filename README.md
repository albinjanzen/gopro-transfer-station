# GoPro Transfer Station

Automated USB transfer station for GoPro footage. Connect a camera, walk away — files are downloaded and converted in the background while a local web UI shows live progress.

## Features

- **Auto-detects** GoPro cameras connected via USB (polls every 2 seconds)
- **Downloads** today's footage and converts `.360` files to equirectangular MP4
- **Web UI** at `http://localhost:8080` with live status, file progress, and a video player
- **Clips page** at `http://localhost:8080/clips` to browse, play, and curate clips by date
- **Safe-to-disconnect** banner appears as soon as downloads finish (processing continues in the background)
- **Skygod Worthy** — save highlight clips to a named folder via the web UI
- Resilient: never exits on error, always waits for the next camera

## Requirements

- macOS (USB detection uses `ioreg`)
- Python 3.11+
- `ffmpeg` available in `$PATH`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python transfer_360.py
```

Then open [http://localhost:8080](http://localhost:8080) in a browser. Connect a GoPro in USB mode — transfer starts automatically.

**Options:**

| Flag | Description |
|---|---|
| `--serial XXXX` | Target a specific camera by serial suffix (auto-discovers if omitted) |
| `--verbose` / `-v` | Enable debug logging |

## Output structure

```
~/Movies/GoPro360/
├── raw/                          # Temporary staging (cleared after each session)
├── processed/
│   └── YYYY-MM-DD/
│       ├── GS010041_<serial>_YYYY-MM-DD.mp4   # Converted 360 footage
│       ├── GX010042_<serial>_YYYY-MM-DD.mp4   # Standard footage (copied as-is)
│       └── .session.json                      # Camera metadata for this session
└── skygodVideos/
    └── <camera name>/
        └── <clip name>_YYYY-MM-DD.mp4         # Curated highlight clips
```

## Web UI

### Transfer page (`/`)

Shows the currently connected camera (name, model, serial), transfer phase, per-file progress, and today's stats. A green banner appears as soon as it is safe to unplug the camera.

### Clips page (`/clips`)

Browse all processed dates, play clips back-to-back with full media controls, and save highlights as Skygod Worthy. Saved clips are organised under `skygodVideos/<camera name>/`.

## Camera setup

Enable USB control on the GoPro:

**HERO12 / HERO11:** Settings → Connections → USB Connection → MTP + Control

The camera must be on and unlocked when plugged in.
