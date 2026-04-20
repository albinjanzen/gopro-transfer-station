#!/usr/bin/env python3
"""Download today's videos from a GoPro via USB and convert/copy to the processed folder.

Supports both GoPro Max (360) and regular GoPros (MP4).

Usage:
    python transfer_360.py [--verbose]

Requirements:
    - open_gopro installed (pip install open-gopro)
    - ffmpeg (brew install ffmpeg)
    - aiohttp (pip install aiohttp)
"""

import argparse
import asyncio
import json
import logging
import os
import shutil
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from pathlib import Path

import usb.core

from open_gopro import WiredGoPro, WirelessGoPro
from open_gopro.domain.exceptions import FailedToFindDevice
from open_gopro.network.wifi.controller import SsidState, WifiController
from returns.result import Failure
import open_gopro.gopro_base as _gopro_base
import aiohttp.web


class UnexpectedDisconnect(Exception):
    """Raised by the USB watchdog when the camera disappears mid-transfer."""


class _NoopWifiController(WifiController):
    """Stub WiFi controller used when connecting via COHN (no WiFi AP needed).

    The real NetworksetupWireless runs macOS CLI tools like `system_profiler
    SPAirPortDataType` synchronously, which blocks the asyncio event loop for
    2-5 seconds. Since we only use BLE+COHN (never WiFi AP mode), the controller
    is never actually used — but open_gopro still instantiates and queries it.
    This stub returns sensible no-op values without running any subprocesses.
    """

    def __init__(self, interface: str | None = None, password: str | None = None) -> None:
        super().__init__(interface, password)
        self._interface = interface or "en0"

    def available_interfaces(self) -> list[str]:
        return [self._interface]

    async def connect(self, ssid: str, password: str, timeout: float = 15) -> bool:
        return False

    async def disconnect(self) -> bool:
        return True

    def current(self) -> tuple[str | None, SsidState]:
        return (None, SsidState.DISCONNECTED)

    @property
    def is_on(self) -> bool:
        return True

    def power(self, power: bool) -> bool:
        return True

# The default HTTP timeout of 5 s is too short for cameras that are slow to respond on connect
# or for large file transfers over USB. Raise it for all HTTP operations.
# HTTP_TIMEOUT is baked into method default parameters at class-definition time, so patching the
# class attribute has no effect. Instead patch __kwdefaults__ on each wrapped method directly.
_HTTP_TIMEOUT = 30
for _method_name in ("_get_json", "_get_stream", "_put_json"):
    _fn = getattr(getattr(_gopro_base.GoProBase, _method_name, None), "__wrapped__", None)
    if _fn and _fn.__kwdefaults__:
        _fn.__kwdefaults__["timeout"] = _HTTP_TIMEOUT


OUTPUT_DIR = Path.home() / "Movies" / "GoPro360"
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"
SKYGOD_DIR = OUTPUT_DIR / "skygodVideos"

FFMPEG = "ffmpeg"
VIDEO_EXTENSIONS = {".360", ".mp4"}
N_PROCESS_WORKERS = 1
COOLDOWN_HOURS = 2

COHN_DB = Path.home() / ".config" / "gopro-transfer" / "cohn.db"
CAMERAS_REGISTRY = Path.home() / ".config" / "gopro-transfer" / "cameras.json"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class FileEntry:
    filename: str
    status: str = "queued"   # queued | downloading | processing | done | error
    size_mb: float | None = None
    error: str | None = None


@dataclass
class AppState:
    phase: str = "waiting"
    # waiting      — scanning for USB or Wi-Fi/BLE device
    # connecting   — SDK context open, before file list fetched
    # transferring — run_downloader active
    # processing   — downloader done, workers still running
    # safe         — everything done, safe to unplug/disconnect
    # error        — unexpected error this cycle

    connection_type: str | None = None   # "usb" | "cohn" | None

    camera_serial: str | None = None
    camera_name: str | None = None
    camera_model: str | None = None
    files: list = field(default_factory=list)   # list[FileEntry]
    files_today_count: int = 0
    files_today_size_mb: float = 0.0

    # Cooldown map: serial → datetime when camera becomes available again
    cooldowns: dict = field(default_factory=dict)

    # SSE subscriber queues — excluded from JSON serialisation
    _sse_queues: list = field(default_factory=list)

    def file_entry(self, filename: str) -> FileEntry:
        name = Path(filename).name
        for e in self.files:
            if Path(e.filename).name == name:
                return e
        raise KeyError(filename)


def _state_to_json(state: AppState) -> str:
    return json.dumps({
        "phase": state.phase,
        "connection_type": state.connection_type,
        "camera_serial": state.camera_serial,
        "camera_name": state.camera_name,
        "camera_model": state.camera_model,
        "files": [asdict(f) for f in state.files],
        "files_today_count": state.files_today_count,
        "files_today_size_mb": state.files_today_size_mb,
    })


def push_event(state: AppState) -> None:
    payload = _state_to_json(state)
    for q in list(state._sse_queues):
        q.put_nowait(payload)


# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GoPro Transfer</title>
<style>
  :root {
    --bg: #111;
    --surface: #1c1c1e;
    --border: #2c2c2e;
    --text: #e5e5e7;
    --muted: #636366;
    --blue: #0a84ff;
    --amber: #ff9f0a;
    --green: #30d158;
    --red: #ff453a;
    --grey: #48484a;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 15px;
    padding: 24px 16px;
  }
  .wrap { max-width: 680px; margin: 0 auto; }

  /* Video player */
  #player-section {
    display: none;
    margin-bottom: 20px;
    border-radius: 10px;
    overflow: hidden;
    background: #000;
    aspect-ratio: 16/9;
  }
  #player {
    width: 100%;
    height: 100%;
    object-fit: contain;
    display: block;
  }

  /* Header */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
  }
  h1 { font-size: 18px; font-weight: 600; letter-spacing: -.2px; }
  .badge {
    font-size: 12px;
    font-weight: 600;
    letter-spacing: .4px;
    text-transform: uppercase;
    padding: 3px 10px;
    border-radius: 20px;
    background: var(--grey);
    color: #fff;
  }
  .badge.phase-connecting   { background: var(--blue); }
  .badge.phase-transferring { background: var(--amber); color: #000; }
  .badge.phase-processing   { background: var(--amber); color: #000; }
  .badge.phase-safe         { background: var(--green); color: #000; }
  .badge.phase-error        { background: var(--red); }

  /* Safe banner */
  #safe-banner {
    display: none;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 20px;
    font-size: 17px;
    font-weight: 700;
    text-align: center;
    letter-spacing: -.1px;
    background: var(--green);
    color: #000;
  }
  #safe-banner.info {
    background: var(--blue);
    color: #fff;
  }

  /* Camera info */
  #camera-info {
    display: none;
    gap: 16px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 20px;
    font-size: 13px;
    color: var(--muted);
  }
  #camera-info span { color: var(--text); font-weight: 500; }
  #camera-info .label { margin-right: 4px; }

  /* Stats */
  #stats {
    display: none;
    gap: 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 20px;
    font-size: 13px;
  }
  #stats .stat-label { color: var(--muted); margin-right: 5px; }
  #stats .stat-value { color: var(--text); font-weight: 600; }

  /* File list */
  #files-section { }
  .section-title {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .6px;
    color: var(--muted);
    margin-bottom: 10px;
  }
  #file-list { display: flex; flex-direction: column; gap: 6px; }
  .file-row {
    display: flex;
    align-items: center;
    gap: 10px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 14px;
  }
  .filename {
    flex: 1;
    font-size: 13px;
    font-family: "SF Mono", "Fira Mono", monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .pill {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .4px;
    padding: 2px 8px;
    border-radius: 20px;
    white-space: nowrap;
  }
  .status-queued     { background: var(--grey);  color: #fff; }
  .status-downloading{ background: var(--blue);  color: #fff; animation: pulse 1.2s infinite; }
  .status-processing { background: var(--amber); color: #000; animation: pulse 1.2s infinite; }
  .status-done       { background: var(--green); color: #000; }
  .status-error      { background: var(--red);   color: #fff; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.55} }
  .size { font-size: 12px; color: var(--muted); white-space: nowrap; }
  .err  { font-size: 11px; color: var(--red); flex: 1; text-align: right;
          white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  #empty { color: var(--muted); font-size: 14px; padding: 12px 0; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>GoPro Transfer</h1>
    <div style="display:flex;align-items:center;gap:16px">
      <span id="phase-badge" class="badge">waiting</span>
      <a href="/clips" style="font-size:13px;color:var(--muted);text-decoration:none">Clips ›</a>
      <a href="/cameras" style="font-size:13px;color:var(--muted);text-decoration:none">Cameras ›</a>
    </div>
  </header>

  <div id="player-section">
    <video id="player" autoplay muted playsinline></video>
  </div>

  <div id="safe-banner">&#10003;&nbsp; Safe to unplug camera</div>

  <div id="camera-info">
    <span class="label" style="color:var(--muted)">Camera</span>
    <span id="camera-name">—</span>
    <span id="camera-model" style="color:var(--muted);font-weight:400"></span>
    <span id="serial" style="color:var(--muted);font-weight:400"></span>
  </div>

  <div id="stats">
    <span class="stat-label">Today</span>
    <span id="stat-count" class="stat-value">0 files</span>
    <span class="stat-label" style="margin-left:8px">Total</span>
    <span id="stat-size" class="stat-value">0 MB</span>
  </div>

  <div id="files-section" style="display:none">
    <div class="section-title" id="files-title">Files</div>
    <div id="file-list"></div>
  </div>
</div>

<script>
const PHASE_LABELS = {
  waiting: 'Waiting', connecting: 'Connecting',
  transferring: 'Transferring', processing: 'Processing',
  safe: 'Done', error: 'Error',
};

function render(s) {
  // Phase badge
  const badge = document.getElementById('phase-badge');
  badge.textContent = PHASE_LABELS[s.phase] || s.phase;
  badge.className = 'badge phase-' + s.phase;

  // Safe banner
  const banner = document.getElementById('safe-banner');
  if (s.phase === 'safe' || s.phase === 'processing') {
    banner.style.display = 'block';
    const noFiles = s.files.length === 0;
    const isCohn = s.connection_type === 'cohn';
    if (noFiles) {
      banner.textContent = '\u2139\ufe0f  No new videos found \u00b7 Safe to ' + (isCohn ? 'disconnect' : 'unplug');
    } else if (s.phase === 'processing') {
      banner.textContent = '\u2699\ufe0f  Processing\u2026 ' + (isCohn ? 'Camera can disconnect' : 'Safe to unplug camera');
    } else {
      banner.textContent = '\u2713  ' + (isCohn ? 'All videos transferred' : 'Safe to unplug camera');
    }
    banner.className = noFiles ? 'info' : '';
  } else {
    banner.style.display = 'none';
  }

  // Camera info
  const camInfo = document.getElementById('camera-info');
  if (s.camera_serial) {
    camInfo.style.display = 'flex';
    document.getElementById('camera-name').textContent = s.camera_name || s.camera_serial;
    document.getElementById('camera-model').textContent = s.camera_model || '';
    document.getElementById('serial').textContent = s.camera_serial || '';
  } else {
    camInfo.style.display = 'none';
  }

  // Stats
  const statsEl = document.getElementById('stats');
  if (s.files_today_count > 0) {
    statsEl.style.display = 'flex';
    document.getElementById('stat-count').textContent =
      s.files_today_count + (s.files_today_count === 1 ? ' file' : ' files');
    const mb = s.files_today_size_mb;
    document.getElementById('stat-size').textContent =
      mb >= 1000 ? (mb / 1000).toFixed(2) + ' GB' : mb.toFixed(0) + ' MB';
  } else {
    statsEl.style.display = 'none';
  }

  // File list
  const section = document.getElementById('files-section');
  const list = document.getElementById('file-list');
  if (!s.files || s.files.length === 0) {
    section.style.display = 'none';
    return;
  }
  section.style.display = 'block';
  document.getElementById('files-title').textContent =
    'Files (' + s.files.length + ')';
  list.innerHTML = s.files.map(f => {
    const size = f.size_mb != null
      ? '<span class="size">' + f.size_mb.toFixed(1) + ' MB</span>' : '';
    const err = f.error
      ? '<span class="err" title="' + esc(f.error) + '">' + esc(f.error) + '</span>' : '';
    return '<div class="file-row">' +
      '<span class="filename">' + esc(f.filename) + '</span>' +
      '<span class="pill status-' + f.status + '">' + f.status + '</span>' +
      size + err +
      '</div>';
  }).join('');
}

function esc(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// Initial state fetch so the page isn't blank before SSE connects
fetch('/state').then(r => r.json()).then(render).catch(() => {});

// SSE stream — also refreshes the video list when files complete
const src = new EventSource('/events');
src.onmessage = e => { render(JSON.parse(e.data)); refreshVideos(); };
src.onerror = () => {};

// Video player
const playerEl = document.getElementById('player');
const playerSection = document.getElementById('player-section');
let playerVideos = [];
let playerIndex = 0;

async function refreshVideos() {
  try {
    const list = await fetch('/videos').then(r => r.json());
    if (list.length === 0) { playerSection.style.display = 'none'; return; }
    playerSection.style.display = 'block';
    const currentName = playerVideos[playerIndex];
    playerVideos = list;
    const stillAt = list.indexOf(currentName);
    if (stillAt === -1 || playerEl.paused) {
      playerIndex = stillAt === -1 ? 0 : stillAt;
      playVideo();
    } else {
      playerIndex = stillAt;
    }
  } catch(e) {}
}

function playVideo() {
  if (!playerVideos.length) return;
  playerEl.src = '/video/' + encodeURIComponent(playerVideos[playerIndex]);
  playerEl.play().catch(() => {});
}

playerEl.addEventListener('ended', () => {
  playerIndex = (playerIndex + 1) % playerVideos.length;
  playVideo();
});
playerEl.addEventListener('error', () => {
  if (playerVideos.length > 1) { playerIndex = (playerIndex + 1) % playerVideos.length; playVideo(); }
});

refreshVideos();
</script>
</body>
</html>
"""


CLIPS_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Clips of the Day</title>
<style>
  :root {
    --bg: #111; --surface: #1c1c1e; --border: #2c2c2e;
    --text: #e5e5e7; --muted: #636366;
    --blue: #0a84ff; --green: #30d158; --grey: #48484a;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 15px; padding: 24px 16px;
  }
  .wrap { max-width: 680px; margin: 0 auto; }
  header {
    display: flex; align-items: center;
    justify-content: space-between; margin-bottom: 20px;
  }
  h1 { font-size: 18px; font-weight: 600; letter-spacing: -.2px; }
  a.nav { font-size: 13px; color: var(--muted); text-decoration: none; }
  a.nav:hover { color: var(--text); }
  .badge {
    font-size: 12px; font-weight: 600; letter-spacing: .4px;
    text-transform: uppercase; padding: 3px 10px;
    border-radius: 20px; background: var(--grey); color: #fff;
  }
  .badge.phase-connecting   { background: var(--blue); }
  .badge.phase-transferring,
  .badge.phase-processing   { background: #ff9f0a; color: #000; }
  .badge.phase-safe         { background: var(--green); color: #000; }
  .badge.phase-error        { background: #ff453a; }

  /* Date selector */
  .date-row {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 16px;
  }
  .date-row label { font-size: 13px; color: var(--muted); }
  select {
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 6px 10px; font-size: 14px; outline: none; cursor: pointer;
  }

  /* Video player */
  #player-wrap {
    border-radius: 10px; overflow: hidden;
    background: #000; aspect-ratio: 16/9; margin-bottom: 16px;
  }
  #player { width: 100%; height: 100%; object-fit: contain; display: block; }

  /* Clip list */
  .section-title {
    font-size: 12px; font-weight: 600; text-transform: uppercase;
    letter-spacing: .6px; color: var(--muted); margin-bottom: 10px;
  }
  #clip-list { display: flex; flex-direction: column; gap: 6px; }
  .clip-row {
    display: flex; align-items: center; gap: 10px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 14px; cursor: pointer;
  }
  .clip-row:hover { border-color: var(--muted); }
  .clip-row.active { border-color: var(--blue); }
  .clip-icon { font-size: 12px; color: var(--blue); width: 14px; text-align: center; }
  .clip-name {
    flex: 1; font-size: 13px;
    font-family: "SF Mono", "Fira Mono", monospace;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .clip-size { font-size: 12px; color: var(--muted); white-space: nowrap; }
  #empty { color: var(--muted); font-size: 14px; padding: 12px 0; }
  #skygod-row {
    display: none; gap: 8px; margin-bottom: 16px; align-items: center;
  }
  #skygod-name {
    flex: 1; background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 10px 12px; font-size: 14px; outline: none;
  }
  #skygod-name:focus { border-color: var(--blue); }
  #skygod-name.invalid { border-color: #ff453a; }
  #skygod-btn {
    padding: 10px 16px; border-radius: 8px; border: none; cursor: pointer;
    background: linear-gradient(135deg, #ff9f0a, #ff6b00);
    color: #000; font-size: 14px; font-weight: 700; white-space: nowrap;
    transition: opacity .15s;
  }
  #skygod-btn:hover { opacity: .85; }
  #skygod-btn:active { opacity: .7; }
  #skygod-btn.saved { background: var(--green); color: #000; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Clips of the Day</h1>
    <div style="display:flex;align-items:center;gap:16px">
      <span id="phase-badge" class="badge">waiting</span>
      <a href="/" class="nav">‹ Transfer</a>
    </div>
  </header>

  <div class="date-row">
    <label for="date-select">Date</label>
    <select id="date-select"></select>
  </div>

  <div id="player-wrap">
    <video id="player" autoplay muted controls playsinline></video>
  </div>

  <div id="skygod-row">
    <input id="skygod-name" type="text" placeholder="Enter a name for this clip…">
    <button id="skygod-btn" onclick="saveSkygod()">Skygod worthy</button>
  </div>

  <div id="clips-section" style="display:none">
    <div class="section-title" id="clips-title">Clips</div>
    <div id="clip-list"></div>
  </div>
  <div id="empty" style="display:none">No clips found for this date.</div>
</div>

<script>
const playerEl = document.getElementById('player');
let clips = [];
let currentDate = '';
let currentIndex = 0;

async function loadDates() {
  const dates = await fetch('/dates').then(r => r.json());
  const sel = document.getElementById('date-select');
  sel.innerHTML = dates.map(d => `<option value="${d}">${d}</option>`).join('');
  if (dates.length) loadDate(dates[0]);
  sel.onchange = () => loadDate(sel.value);
}

async function loadDate(date, autoplay = true) {
  currentDate = date;
  clips = await fetch('/clips/videos/' + encodeURIComponent(date)).then(r => r.json());
  renderClipList();
  if (clips.length) { if (autoplay) playClip(0); }
  else {
    playerEl.src = '';
    document.getElementById('clips-section').style.display = 'none';
    document.getElementById('empty').style.display = 'block';
  }
}

function renderClipList() {
  const section = document.getElementById('clips-section');
  const empty = document.getElementById('empty');
  const list = document.getElementById('clip-list');
  if (!clips.length) { section.style.display = 'none'; empty.style.display = 'block'; return; }
  section.style.display = 'block';
  empty.style.display = 'none';
  document.getElementById('clips-title').textContent = 'Clips (' + clips.length + ')';
  list.innerHTML = clips.map((c, i) => {
    const size = c.size_mb >= 1000
      ? (c.size_mb / 1000).toFixed(2) + ' GB'
      : c.size_mb.toFixed(0) + ' MB';
    return '<div class="clip-row" id="clip-' + i + '" onclick="playClip(' + i + ')">' +
      '<span class="clip-icon"></span>' +
      '<span class="clip-name">' + esc(c.name) + '</span>' +
      '<span class="clip-size">' + size + '</span>' +
      '</div>';
  }).join('');
  highlightClip(currentIndex);
}

function playClip(index) {
  currentIndex = index;
  playerEl.src = '/clips/video/' + encodeURIComponent(currentDate) + '/' + encodeURIComponent(clips[index].name);
  playerEl.play().catch(() => {});
  highlightClip(index);
  const row = document.getElementById('skygod-row');
  const btn = document.getElementById('skygod-btn');
  const inp = document.getElementById('skygod-name');
  row.style.display = 'flex';
  inp.value = '';
  inp.classList.remove('invalid');
  btn.textContent = 'Skygod worthy';
  btn.classList.remove('saved');
}

async function saveSkygod() {
  const btn = document.getElementById('skygod-btn');
  const inp = document.getElementById('skygod-name');
  if (!clips.length) return;
  const name = inp.value.trim();
  if (!name) {
    inp.classList.add('invalid');
    inp.focus();
    return;
  }
  inp.classList.remove('invalid');
  btn.disabled = true;
  try {
    const url = '/clips/skygod/' + encodeURIComponent(currentDate) + '/' +
      encodeURIComponent(clips[currentIndex].name) + '?name=' + encodeURIComponent(name);
    const res = await fetch(url, { method: 'POST' });
    if (res.ok) {
      btn.textContent = 'Saved!';
      btn.classList.add('saved');
    } else if (res.status === 409) {
      btn.textContent = await res.text();
      btn.classList.add('saved');
      btn.disabled = true;
    } else {
      btn.textContent = 'Error — try again';
    }
  } catch {
    btn.textContent = 'Error — try again';
  }
  btn.disabled = false;
}

function highlightClip(index) {
  document.querySelectorAll('.clip-row').forEach((r, i) => {
    r.classList.toggle('active', i === index);
    r.querySelector('.clip-icon').textContent = i === index ? '▶' : '';
  });
}

playerEl.addEventListener('ended', () => playClip((currentIndex + 1) % clips.length));
playerEl.addEventListener('error', () => {
  if (clips.length > 1) playClip((currentIndex + 1) % clips.length);
});

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

document.getElementById('skygod-name').addEventListener('input', () => playerEl.pause());

loadDates();
fetch('/state').then(r => r.json()).then(s => {
  const badge = document.getElementById('phase-badge');
  badge.textContent = PHASE_LABELS[s.phase] || s.phase;
  badge.className = 'badge phase-' + s.phase;
}).catch(() => {});

// Refresh clip list when new files finish processing
let prevDoneCount = 0;
const evtSrc = new EventSource('/events');
const PHASE_LABELS = {
  waiting: 'Waiting', connecting: 'Connecting',
  transferring: 'Transferring', processing: 'Processing',
  safe: 'Done', error: 'Error',
};
evtSrc.onmessage = async e => {
  const s = JSON.parse(e.data);
  const badge = document.getElementById('phase-badge');
  badge.textContent = PHASE_LABELS[s.phase] || s.phase;
  badge.className = 'badge phase-' + s.phase;
  const doneCount = (s.files || []).filter(f => f.status === 'done').length;
  if (doneCount > prevDoneCount) {
    prevDoneCount = doneCount;
    // Refresh date list — today's folder may have appeared for the first time
    const dates = await fetch('/dates').then(r => r.json()).catch(() => []);
    const sel = document.getElementById('date-select');
    sel.innerHTML = dates.map(d => `<option value="${d}">${d}</option>`).join('');
    const today = new Date().toISOString().split('T')[0];
    const target = dates.includes(today) ? today : (dates.length ? dates[0] : null);
    if (target) {
      sel.value = target;
      loadDate(target, target !== currentDate);
    }
  }
  if (!s.files || s.files.length === 0) prevDoneCount = 0;
};
evtSrc.onerror = () => {};
</script>
</body>
</html>
"""


CAMERAS_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cameras</title>
<style>
  :root {
    --bg: #111; --surface: #1c1c1e; --border: #2c2c2e;
    --text: #e5e5e7; --muted: #636366;
    --blue: #0a84ff; --green: #30d158; --red: #ff453a; --grey: #48484a;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 15px; padding: 24px 16px;
  }
  .wrap { max-width: 680px; margin: 0 auto; }
  header {
    display: flex; align-items: center;
    justify-content: space-between; margin-bottom: 20px;
  }
  h1 { font-size: 18px; font-weight: 600; letter-spacing: -.2px; }
  a.nav { font-size: 13px; color: var(--muted); text-decoration: none; }
  a.nav:hover { color: var(--text); }
  .section-title {
    font-size: 12px; font-weight: 600; text-transform: uppercase;
    letter-spacing: .6px; color: var(--muted); margin-bottom: 10px;
  }
  #camera-list { display: flex; flex-direction: column; gap: 8px; }
  .cam-row {
    display: flex; align-items: center; gap: 12px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 16px;
  }
  .cam-row.active { border-color: var(--green); }
  .cam-info { flex: 1; display: flex; flex-direction: column; gap: 4px; }
  .cam-serial { font-size: 14px; font-weight: 600; }
  .cam-ip { font-size: 12px; color: var(--muted); font-family: "SF Mono","Fira Mono",monospace; }
  .cam-status {
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: .4px; padding: 2px 8px; border-radius: 20px;
    background: var(--grey); color: #fff; white-space: nowrap;
  }
  .cam-status.connected { background: var(--green); color: #000; }
  .cam-status.cooldown  { background: #ff9f0a; color: #000; }
  .badge {
    font-size: 12px; font-weight: 600; letter-spacing: .4px;
    text-transform: uppercase; padding: 3px 10px;
    border-radius: 20px; background: var(--grey); color: #fff;
  }
  .badge.phase-connecting   { background: var(--blue); }
  .badge.phase-transferring,
  .badge.phase-processing   { background: #ff9f0a; color: #000; }
  .badge.phase-safe         { background: var(--green); color: #000; }
  .badge.phase-error        { background: var(--red); }
  .reset-btn {
    background: none; border: 1px solid var(--border); border-radius: 8px;
    color: #ff9f0a; font-size: 12px; font-weight: 600; padding: 6px 12px;
    cursor: pointer; white-space: nowrap;
  }
  .reset-btn:hover { background: #ff9f0a; color: #000; border-color: #ff9f0a; }
  .remove-btn {
    background: none; border: 1px solid var(--border); border-radius: 8px;
    color: var(--red); font-size: 12px; font-weight: 600; padding: 6px 12px;
    cursor: pointer; white-space: nowrap;
  }
  .remove-btn:hover { background: var(--red); color: #fff; border-color: var(--red); }
  #empty { color: var(--muted); font-size: 14px; padding: 12px 0; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Cameras</h1>
    <div style="display:flex;align-items:center;gap:16px">
      <span id="phase-badge" class="badge">waiting</span>
      <a href="/" class="nav">&#8249; Transfer</a>
    </div>
  </header>
  <div class="section-title">Provisioned cameras</div>
  <div id="camera-list"><p id="empty">No cameras provisioned yet.</p></div>
</div>
<script>
const PHASE_LABELS = {
  waiting: 'Waiting', connecting: 'Connecting',
  transferring: 'Transferring', processing: 'Processing',
  safe: 'Done', error: 'Error',
};

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function setBadge(phase) {
  const badge = document.getElementById('phase-badge');
  badge.textContent = PHASE_LABELS[phase] || phase;
  badge.className = 'badge phase-' + phase;
}

let _currentSerial = null;

async function loadCameras(activeSerial) {
  try {
    const cameras = await fetch('/cameras/list').then(r => r.json());
    const list = document.getElementById('camera-list');
    if (!cameras.length) {
      list.innerHTML = '<p id="empty">No cameras provisioned yet.</p>';
      return;
    }
    list.innerHTML = cameras.map(c => {
      const isActive = activeSerial && activeSerial === c.serial;
      const displayName = c.camera_name || c.serial;
      const types = (c.connection_types || []).join(', ').toUpperCase() || '—';
      const meta = [c.model, c.serial, c.ip_address].filter(Boolean).join(' · ');
      const lastSeen = c.last_seen ? 'Last seen ' + c.last_seen.replace('T', ' ') : '';
      const cooldown = c.cooldown_until ? 'Next sync after ' + c.cooldown_until.replace('T', ' ') : '';
      const onCooldown = !!c.cooldown_until && new Date(c.cooldown_until) > new Date();
      return '<div class="cam-row' + (isActive ? ' active' : '') + '">' +
        '<div class="cam-info">' +
          '<span class="cam-serial">' + esc(displayName) + ' <span style="font-weight:400;color:var(--muted);font-size:12px">' + esc(types) + '</span></span>' +
          '<span class="cam-ip">' + esc(meta) + '</span>' +
          (lastSeen ? '<span class="cam-ip">' + esc(lastSeen) + '</span>' : '') +
          (cooldown ? '<span class="cam-ip" style="color:' + (onCooldown ? '#ff9f0a' : 'var(--muted)') + '">' + esc(cooldown) + '</span>' : '') +
        '</div>' +
        '<span class="cam-status' + (isActive ? ' connected' : (onCooldown ? ' cooldown' : '')) + '">' +
          (isActive ? 'Connected' : (onCooldown ? 'Cooldown' : 'Known')) + '</span>' +
        (onCooldown ? '<button class="reset-btn" data-serial="' + esc(c.serial) + '" onclick="resetCooldown(this.dataset.serial)">Reset cooldown</button>' : '') +
        '<button class="remove-btn" data-serial="' + esc(c.serial) + '" onclick="remove(this.dataset.serial)">Remove</button>' +
      '</div>';
    }).join('');
  } catch(e) {}
}

async function resetCooldown(serial) {
  await fetch('/cameras/cooldown/' + encodeURIComponent(serial), { method: 'DELETE' });
  loadCameras(_currentSerial);
}

async function remove(serial) {
  if (!confirm('Remove camera ' + serial + ' from the list?')) return;
  await fetch('/cameras/remove/' + encodeURIComponent(serial), { method: 'DELETE' });
  loadCameras(_currentSerial);
}

// Initial load
fetch('/state').then(r => r.json()).then(s => {
  setBadge(s.phase);
  _currentSerial = s.camera_serial;
  loadCameras(s.camera_serial);
}).catch(() => {});

// SSE: live badge + refresh cameras list when a new camera connects/disconnects
const src = new EventSource('/events');
src.onmessage = e => {
  const s = JSON.parse(e.data);
  setBadge(s.phase);
  if (s.camera_serial !== _currentSerial) {
    _currentSerial = s.camera_serial;
    loadCameras(s.camera_serial);
  }
};
src.onerror = () => {};
</script>
</body>
</html>
"""


async def _cameras_list_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    cameras = []
    if CAMERAS_REGISTRY.exists():
        try:
            cameras = json.loads(CAMERAS_REGISTRY.read_text())
        except Exception:
            pass
    # Enrich COHN cameras with IP address from the SDK DB.
    if COHN_DB.exists():
        try:
            import tinydb
            db = tinydb.TinyDB(COHN_DB)
            for record in db.all():
                full_serial = record.get("full_serial", "")
                short_serial = record.get("serial", "")
                ip = record.get("credentials", {}).get("ip_address", "")
                for cam in cameras:
                    if cam["serial"] == full_serial or cam["serial"].endswith(short_serial):
                        cam["ip_address"] = ip
                        break
            db.close()
        except Exception:
            pass
    # Enrich with cooldown expiry from live state.
    state: AppState = request.app["state"]
    for cam in cameras:
        expiry = state.cooldowns.get(cam["serial"])
        if expiry:
            cam["cooldown_until"] = expiry.isoformat(timespec="seconds") if isinstance(expiry, datetime) else expiry
    return aiohttp.web.json_response(cameras)


async def _cameras_cooldown_reset_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    serial = request.match_info["serial"]
    state: AppState = request.app["state"]
    state.cooldowns.pop(serial, None)
    log.info("Cooldown reset for camera %s.", serial)
    return aiohttp.web.Response(status=204)


async def _cameras_remove_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    serial = request.match_info["serial"]
    # Remove from registry.
    if CAMERAS_REGISTRY.exists():
        try:
            cameras = json.loads(CAMERAS_REGISTRY.read_text())
            cameras = [c for c in cameras if c.get("serial") != serial]
            CAMERAS_REGISTRY.write_text(json.dumps(cameras, indent=2))
        except Exception:
            pass
    # Remove COHN credentials if present (match on full or short serial).
    if COHN_DB.exists():
        try:
            import tinydb
            db = tinydb.TinyDB(COHN_DB)
            db.remove(tinydb.Query().serial.test(lambda s: serial.endswith(s) or s == serial))
            db.close()
        except Exception:
            pass
    log.info("Removed camera %s from registry.", serial)
    return aiohttp.web.Response(status=204)


async def _dates_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    if not PROCESSED_DIR.exists():
        return aiohttp.web.json_response([])
    dates = sorted(
        (d.name for d in PROCESSED_DIR.iterdir() if d.is_dir()),
        reverse=True,
    )
    return aiohttp.web.json_response(dates)


async def _clips_video_list_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    date = request.match_info["date"]
    if "/" in date or ".." in date:
        raise aiohttp.web.HTTPForbidden()
    folder = PROCESSED_DIR / date
    if not folder.is_dir():
        return aiohttp.web.json_response([])
    videos = sorted(
        ({"name": f.name, "size_mb": f.stat().st_size / 1_000_000}
         for f in folder.iterdir()
         if f.is_file() and f.suffix.lower() == ".mp4"),
        key=lambda v: v["name"],
    )
    return aiohttp.web.json_response(videos)


async def _clips_video_file_handler(request: aiohttp.web.Request) -> aiohttp.web.FileResponse:
    date = request.match_info["date"]
    filename = request.match_info["filename"]
    if any("/" in s or "\\" in s or ".." in s for s in (date, filename)):
        raise aiohttp.web.HTTPForbidden()
    path = PROCESSED_DIR / date / filename
    if not path.is_file():
        raise aiohttp.web.HTTPNotFound()
    return aiohttp.web.FileResponse(path)


def _skygod_sidecar(src: Path) -> Path:
    """Path of the hidden sidecar file that records the saved skygod name for src."""
    return src.parent / f".{src.stem}.skygod"


def _update_camera_registry(state: AppState) -> None:
    """Upsert this camera into the persistent cameras registry."""
    if not state.camera_serial:
        return
    CAMERAS_REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    cameras = []
    if CAMERAS_REGISTRY.exists():
        try:
            cameras = json.loads(CAMERAS_REGISTRY.read_text())
        except Exception:
            pass
    entry = next((c for c in cameras if c.get("serial") == state.camera_serial), None)
    if entry is None:
        entry = {"serial": state.camera_serial, "connection_types": []}
        cameras.append(entry)
    entry["camera_name"] = state.camera_name or entry.get("camera_name", "")
    entry["model"] = state.camera_model or entry.get("model", "")
    entry["last_seen"] = datetime.now().isoformat(timespec="seconds")
    if state.connection_type and state.connection_type not in entry["connection_types"]:
        entry["connection_types"].append(state.connection_type)
    CAMERAS_REGISTRY.write_text(json.dumps(cameras, indent=2))


def _write_session_file(today_dir: Path, state: AppState) -> None:
    """Write/update a hidden .session.json in today_dir with this camera's info."""
    session_path = today_dir / ".session.json"
    sessions = []
    if session_path.exists():
        try:
            sessions = json.loads(session_path.read_text())
        except Exception:
            pass
    entry = {
        "serial": state.camera_serial,
        "camera_name": state.camera_name,
        "model": state.camera_model,
    }
    if entry not in sessions:
        sessions.append(entry)
    today_dir.mkdir(parents=True, exist_ok=True)
    session_path.write_text(json.dumps(sessions, indent=2))


async def _skygod_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    date = request.match_info["date"]
    filename = request.match_info["filename"]
    if any("/" in s or "\\" in s or ".." in s for s in (date, filename)):
        raise aiohttp.web.HTTPForbidden()
    custom_name = request.rel_url.query.get("name", "").strip()
    if not custom_name:
        return aiohttp.web.Response(status=400, text="name is required")
    # Sanitise: strip path separators and null bytes, keep only the basename
    custom_name = Path(custom_name).name.replace("\x00", "")
    if not custom_name:
        return aiohttp.web.Response(status=400, text="invalid name")

    src = PROCESSED_DIR / date / filename
    if not src.is_file():
        raise aiohttp.web.HTTPNotFound()

    sidecar = _skygod_sidecar(src)
    if sidecar.exists():
        saved_name = sidecar.read_text().strip()
        return aiohttp.web.Response(status=409, text=f'Already saved as "{saved_name}"')

    # Resolve camera name from session file using the serial embedded in the filename.
    # Filename format: {original_stem}_{serial}_{date}.mp4 → serial is second-to-last segment.
    stem_parts = Path(filename).stem.rsplit("_", 2)
    file_serial = stem_parts[1] if len(stem_parts) == 3 else None
    camera_folder = file_serial  # fallback to serial if no session data
    session_path = PROCESSED_DIR / date / ".session.json"
    if file_serial and session_path.exists():
        try:
            for entry in json.loads(session_path.read_text()):
                if entry.get("serial") == file_serial and entry.get("camera_name"):
                    camera_folder = entry["camera_name"]
                    break
        except Exception:
            pass
    camera_folder = camera_folder or "unknown"

    ext = Path(filename).suffix.lower()
    dest_dir = SKYGOD_DIR / camera_folder
    dest_dir.mkdir(parents=True, exist_ok=True)
    base = f"{custom_name}_{date}"
    dest = dest_dir / f"{base}{ext}"
    counter = 2
    while dest.exists():
        dest = dest_dir / f"{base}_{counter}{ext}"
        counter += 1
    await asyncio.get_event_loop().run_in_executor(None, shutil.copy2, src, dest)
    sidecar.write_text(custom_name)
    log.info("Skygod: saved %s -> %s", src, dest)
    return aiohttp.web.Response(status=200)


async def _video_list_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    today_dir = today_processed_dir()
    if not today_dir.exists():
        return aiohttp.web.json_response([])
    videos = sorted(f.name for f in today_dir.iterdir()
                    if f.is_file() and f.suffix.lower() == ".mp4")
    return aiohttp.web.json_response(videos)


async def _video_file_handler(request: aiohttp.web.Request) -> aiohttp.web.FileResponse:
    filename = request.match_info["filename"]
    if "/" in filename or "\\" in filename or ".." in filename:
        raise aiohttp.web.HTTPForbidden()
    path = today_processed_dir() / filename
    if not path.is_file():
        raise aiohttp.web.HTTPNotFound()
    return aiohttp.web.FileResponse(path)


async def _sse_handler(request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
    state: AppState = request.app["state"]
    queue: asyncio.Queue = asyncio.Queue()
    state._sse_queues.append(queue)
    response = aiohttp.web.StreamResponse(headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await response.prepare(request)
    try:
        # Send current snapshot immediately so new tabs aren't blank
        await response.write(f"data: {_state_to_json(state)}\n\n".encode())
        while True:
            data = await queue.get()
            await response.write(f"data: {data}\n\n".encode())
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    finally:
        try:
            state._sse_queues.remove(queue)
        except ValueError:
            pass
    return response


async def run_web_server(state: AppState, port: int = 8080) -> None:
    app = aiohttp.web.Application()
    app["state"] = state
    app.router.add_get("/", lambda r: aiohttp.web.Response(
        text=HTML_PAGE, content_type="text/html"))
    app.router.add_get("/state", lambda r: aiohttp.web.Response(
        text=_state_to_json(r.app["state"]), content_type="application/json"))
    app.router.add_get("/events", _sse_handler)
    app.router.add_get("/videos", _video_list_handler)
    app.router.add_get("/video/{filename}", _video_file_handler)
    app.router.add_get("/clips", lambda r: aiohttp.web.Response(
        text=CLIPS_PAGE, content_type="text/html"))
    app.router.add_get("/cameras", lambda r: aiohttp.web.Response(
        text=CAMERAS_PAGE, content_type="text/html"))
    app.router.add_get("/cameras/list", _cameras_list_handler)
    app.router.add_delete("/cameras/remove/{serial}", _cameras_remove_handler)
    app.router.add_delete("/cameras/cooldown/{serial}", _cameras_cooldown_reset_handler)
    app.router.add_get("/dates", _dates_handler)
    app.router.add_get("/clips/videos/{date}", _clips_video_list_handler)
    app.router.add_get("/clips/video/{date}/{filename}", _clips_video_file_handler)
    app.router.add_post("/clips/skygod/{date}/{filename}", _skygod_handler)

    runner = aiohttp.web.AppRunner(app, access_log=None)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    log.info("Web UI available at http://127.0.0.1:%d", port)
    try:
        await asyncio.get_event_loop().create_future()  # run forever
    finally:
        await runner.cleanup()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("open_gopro").setLevel(logging.DEBUG if verbose else logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def today_processed_dir() -> Path:
    return PROCESSED_DIR / date.today().isoformat()


def ensure_dirs() -> None:
    for d in (RAW_DIR, PROCESSED_DIR):
        d.mkdir(parents=True, exist_ok=True)
        log.debug("Directory ready: %s", d)


def refresh_today_stats(state: AppState, today_dir: Path) -> None:
    if not today_dir.exists():
        state.files_today_count = 0
        state.files_today_size_mb = 0.0
        return
    video_files = [
        f for f in today_dir.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
    ]
    state.files_today_count = len(video_files)
    state.files_today_size_mb = sum(f.stat().st_size for f in video_files) / 1_000_000


# ---------------------------------------------------------------------------
# Camera / file logic
# ---------------------------------------------------------------------------

async def get_todays_files(gopro, processed_dir: Path, serial: str) -> list:
    log.info("Fetching media list from camera...")
    response = await gopro.http_command.get_media_list()
    if not response.ok:
        log.error("Failed to get media list: %s", response.status)
        return []

    all_files = response.data.files
    log.debug("Total files on camera: %d", len(all_files))

    today_start = datetime.combine(date.today(), datetime.min.time()).timestamp()
    log.debug("Filtering for files created on or after %s", date.today().isoformat())

    files = [
        item
        for item in all_files
        if Path(item.filename).suffix.lower() in VIDEO_EXTENSIONS
        and float(item.creation_timestamp) >= today_start
    ]

    new_files = [
        item for item in files
        if not (processed_dir / f"{Path(item.filename).stem}_{serial}_{processed_dir.name}.mp4").exists()
    ]
    skipped = len(files) - len(new_files)
    if skipped:
        log.info("Skipping %d already-processed file(s).", skipped)

    log.info("Found %d new video file(s) from today.", len(new_files))
    if new_files:
        for f in new_files:
            created = datetime.fromtimestamp(float(f.creation_timestamp)).strftime("%H:%M:%S")
            log.info("  %s  (created %s)", f.filename, created)
    else:
        log.info("All files on camera (%d total):", len(all_files))
        for f in all_files:
            ext = Path(f.filename).suffix
            created = datetime.fromtimestamp(float(f.creation_timestamp)).strftime("%Y-%m-%d %H:%M:%S")
            log.info("  %s  [%s]  created %s", f.filename, ext, created)

    return new_files


async def is_valid_video(path: Path) -> bool:
    """Return True if ffprobe can read a valid duration from the file."""
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return proc.returncode == 0 and b"duration=N/A" not in stdout and b"duration=" in stdout


async def download_file(gopro, item, raw_dir: Path) -> Path:
    dest = raw_dir / Path(item.filename).name

    if dest.exists():
        if await is_valid_video(dest):
            log.info("Skipping download, already exists: %s", dest.name)
            return dest
        log.warning("Incomplete file detected, re-downloading: %s", dest.name)
        dest.unlink()

    log.info("Downloading %s -> %s", item.filename, dest)
    response = await gopro.http_command.download_file(
        camera_file=item.filename,
        local_file=dest,
    )
    if not response.ok:
        raise RuntimeError(f"Download failed for {item.filename}: {response.status}")

    size_mb = dest.stat().st_size / 1_000_000
    log.info("Download complete: %s (%.1f MB)", dest.name, size_mb)
    return dest


async def run_ffmpeg(args: list[str], label: str) -> None:
    log.debug("ffmpeg command: %s", " ".join(args))
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.error("ffmpeg stderr:\n%s", stderr.decode())
        raise RuntimeError(f"ffmpeg failed during {label}")


async def process_360(raw_path: Path, processed_dir: Path, serial: str) -> Path:
    out_path = processed_dir / f"{raw_path.stem}_{serial}_{processed_dir.name}.mp4"

    if out_path.exists():
        log.info("Skipping processing, already exists: %s", out_path.name)
        return out_path

    log.info("Encoding %s...", raw_path.name)
    await run_ffmpeg([
        FFMPEG, "-y",
        "-threads", "4",
        "-i", str(raw_path),
        "-filter_complex", "[0:0][0:4]vstack,v360=eac:equirect,scale=iw*1.2:ih*1.2,crop=iw/1.2:ih/1.2",
        "-map", "0:1",
        "-map", "0:3",
        "-c:v", "libx264",
        "-c:a", "copy",
        str(out_path),
    ], label="encode")

    size_mb = out_path.stat().st_size / 1_000_000
    log.info("Processed: %s (%.1f MB)", out_path.name, size_mb)
    return out_path


async def copy_to_processed(raw_path: Path, processed_dir: Path, serial: str) -> Path:
    out_path = processed_dir / f"{raw_path.stem}_{serial}_{processed_dir.name}{raw_path.suffix.lower()}"

    if out_path.exists():
        log.info("Skipping copy, already exists: %s", out_path.name)
        return out_path

    await asyncio.get_event_loop().run_in_executor(None, shutil.copy2, raw_path, out_path)
    size_mb = out_path.stat().st_size / 1_000_000
    log.info("Copied: %s (%.1f MB)", out_path.name, size_mb)
    return out_path


async def process_file(raw_path: Path, processed_dir: Path, serial: str) -> Path:
    if raw_path.suffix.lower() == ".360":
        result = await process_360(raw_path, processed_dir, serial)
    else:
        result = await copy_to_processed(raw_path, processed_dir, serial)
    raw_path.unlink()
    log.info("Deleted raw file: %s", raw_path.name)
    return result


async def run_downloader(
    gopro, items: list, queue: asyncio.Queue, raw_dir: Path, state: AppState
) -> None:
    """Download files one at a time and enqueue each immediately when done.

    Sequential downloads are intentional: the SDK's HTTP download blocks the event loop
    (requests.iter_content runs synchronously), and a single USB connection can't
    meaningfully parallelize anyway. Enqueueing each file as it finishes lets the
    processor start on it while the next download runs.
    """
    try:
        for item in items:
            entry = state.file_entry(item.filename)
            entry.status = "downloading"
            push_event(state)
            try:
                raw_path = await download_file(gopro, item, raw_dir)
                entry.status = "queued"
                entry.size_mb = raw_path.stat().st_size / 1_000_000
                push_event(state)
                await queue.put(raw_path)
                await asyncio.sleep(0)  # yield to let the processor pick up the file
            except Exception as e:
                log.error("Failed to download %s, skipping: %s", item.filename, e, exc_info=True)
                entry.status = "error"
                entry.error = str(e)
                push_event(state)
    finally:
        await queue.put(None)  # sentinel always sent, even if downloads failed
        state.phase = "processing"  # camera no longer needed; ffmpeg still running
        push_event(state)


async def run_processor(
    queue: asyncio.Queue, processed_dir: Path, n_workers: int, state: AppState, today_dir: Path, serial: str
) -> None:
    """Pull files from the queue and process them with N concurrent workers."""
    async def worker():
        while True:
            raw_path = await queue.get()
            if raw_path is None:
                await queue.put(None)  # re-broadcast sentinel to remaining workers
                return
            entry = state.file_entry(raw_path.name)
            entry.status = "processing"
            push_event(state)
            try:
                await process_file(raw_path, processed_dir, serial)
                entry.status = "done"
                refresh_today_stats(state, today_dir)
                push_event(state)
            except Exception as e:
                log.error("Failed to process %s, skipping: %s", raw_path.name, e, exc_info=True)
                entry.status = "error"
                entry.error = str(e)
                push_event(state)

    await asyncio.gather(*[worker() for _ in range(n_workers)])


# ---------------------------------------------------------------------------
# USB device detection
# ---------------------------------------------------------------------------

GOPRO_USB_VENDOR_ID = 0x2672


async def _has_gopro_usb() -> bool:
    """Check for a GoPro USB device without blocking the event loop."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: usb.core.find(idVendor=GOPRO_USB_VENDOR_ID)
        )
        return result is not None
    except Exception as e:
        log.debug("USB check failed: %s", e)
        return False


async def wait_for_usb_gopro_disconnect(poll_interval: float = 1.0) -> None:
    """Block until no GoPro USB device is present."""
    log.info("Waiting for GoPro to disconnect... (Ctrl+C to stop)")
    while await _has_gopro_usb():
        await asyncio.sleep(poll_interval)
    log.info("GoPro disconnected.")


async def wait_for_usb_gopro(poll_interval: float = 0.5) -> None:
    """Block until a GoPro USB device appears."""
    log.info("Waiting for GoPro USB connection... (Ctrl+C to stop)")
    while not await _has_gopro_usb():
        await asyncio.sleep(poll_interval)
    log.info("GoPro USB device detected.")


def _cleanup_raw_dir() -> None:
    """Delete all files in the raw staging directory after an aborted transfer."""
    if not RAW_DIR.exists():
        return
    for f in RAW_DIR.iterdir():
        if f.is_file():
            try:
                f.unlink()
                log.info("Cleanup: deleted raw file %s", f.name)
            except Exception as e:
                log.warning("Cleanup: could not delete %s: %s", f.name, e)


async def _usb_watchdog(state: AppState, poll_interval: float = 2.0) -> None:
    """Raise UnexpectedDisconnect if the camera disappears before downloads finish.

    Exits silently once state.phase becomes 'safe' (all files are on disk and
    the camera is no longer needed), allowing processing to continue uninterrupted.
    """
    while True:
        await asyncio.sleep(poll_interval)
        if state.phase in ("processing", "safe"):
            return
        if not await _has_gopro_usb():
            raise UnexpectedDisconnect("Camera disconnected during transfer")


async def _wireless_watchdog(gopro, state: AppState, poll_interval: float = 2.0) -> None:
    """Raise UnexpectedDisconnect if BLE connection drops before downloads finish."""
    while True:
        await asyncio.sleep(poll_interval)
        if state.phase in ("processing", "safe"):
            return
        if not gopro.is_ble_connected:
            raise UnexpectedDisconnect("BLE connection lost during transfer")


def _usb_serial_hint() -> str | None:
    """Return a serial to pass to WiredGoPro from the cameras registry.

    If exactly one camera is registered, use its serial so that WiredGoPro can
    skip the 10-second mDNS discovery and derive the USB IP directly.
    """
    try:
        cameras = json.loads(CAMERAS_REGISTRY.read_text()) if CAMERAS_REGISTRY.exists() else []
        serials = [c["serial"] for c in cameras if c.get("serial")]
        if len(serials) == 1:
            return serials[0]
    except Exception:
        pass
    return None


async def _discover_usb() -> tuple:
    """Loop until a USB GoPro is found and connected. Returns (gopro, False)."""
    hint = _usb_serial_hint()
    while True:
        if not await _has_gopro_usb():
            await asyncio.sleep(0.5)
            continue
        # USB device is present — retry open() until the camera's HTTP stack is ready.
        for attempt in range(12):
            try:
                gopro = WiredGoPro(serial=hint, poll_period=0.5)
                await gopro.open()
                return gopro, False
            except FailedToFindDevice:
                log.debug("USB device present but not ready (attempt %d/12), retrying...", attempt + 1)
                await asyncio.sleep(1.0)
            except Exception as e:
                log.debug("USB connect error: %s", e)
                await asyncio.sleep(1.0)
        # If still not ready after ~12 s, fall back to re-checking USB presence.
        log.debug("USB open failed after 12 attempts, re-checking device presence.")


async def _discover_wireless(state: AppState) -> tuple:
    """Connect to a GoPro via COHN (Camera on Home Network). Returns (gopro, True).

    Uses BLE to provision COHN on first use (fetches IP, credentials, certificate
    from the camera and caches them in COHN_DB). Subsequent connections use the
    cached credentials and connect directly via HTTP — no BLE required after that.

    The camera must have COHN set up (e.g., via the GoPro app) before calling this.
    """
    COHN_DB.parent.mkdir(parents=True, exist_ok=True)
    # open_gopro's WiFi driver checks os.environ["LANG"] even in COHN mode — set en_US once.
    os.environ.setdefault("LANG", "en_US.UTF-8")
    if not os.environ["LANG"].startswith("en_US"):
        os.environ["LANG"] = "en_US.UTF-8"
    while True:
        # Before starting a BLE scan, check if all known cameras are still on cooldown.
        # This avoids connecting to (and disturbing) the camera when there is nothing to do.
        cooldowns = state.cooldowns
        if cooldowns and COHN_DB.exists():
            try:
                import tinydb as _tdb
                _db = _tdb.TinyDB(COHN_DB)
                _records = _db.all()
                _db.close()
                if _records:
                    _known = [r.get("full_serial") for r in _records if r.get("full_serial")]
                    if _known and all(
                        cooldowns.get(s) and datetime.now() < cooldowns[s]
                        for s in _known
                    ):
                        _soonest = min(cooldowns[s] for s in _known)
                        log.debug("All provisioned cameras on cooldown until %s — skipping BLE scan.", _soonest.strftime("%H:%M"))
                        await asyncio.sleep(30)
                        continue
            except Exception:
                pass
        try:
            log.info("Attempting COHN connection (Camera on Home Network)...")
            # BLE is always required — the SDK uses BLE to identify the camera and look up
            # its COHN credentials in the DB. COHN-only mode is not supported by the SDK.
            gopro = WirelessGoPro(
                target=None,
                interfaces={WirelessGoPro.Interface.BLE, WirelessGoPro.Interface.COHN},
                cohn_db=COHN_DB,
                wifi_adapter=_NoopWifiController,
            )
            await gopro.open(timeout=10, retries=2)
            # Provision COHN if this is the first time (writes credentials to COHN_DB).
            if not await gopro.cohn.is_configured:
                log.info("COHN not yet provisioned — running first-time setup via BLE (camera must be on home WiFi)...")
                result = await gopro.cohn.configure(timeout=90)
                if isinstance(result, Failure):
                    log.warning("COHN provisioning failed: %s — retrying in 10 s.", result.failure())
                    await gopro.close()
                    await asyncio.sleep(10)
                    continue
                log.info("COHN provisioned and credentials saved to %s.", COHN_DB)
            # Use full serial from HTTP API — matches the key used by main() when storing cooldowns.
            # gopro.identifier returns only the short BLE name ("6313"), not the full serial.
            info_resp = await gopro.http_command.get_camera_info()
            cam_serial = info_resp.data.serial_number if info_resp.ok else gopro.identifier
            expiry = state.cooldowns.get(cam_serial)
            if expiry and datetime.now() < expiry:
                log.info("COHN GoPro %s in cooldown until %s, ignoring.", cam_serial, expiry.strftime("%H:%M"))
                try:
                    await gopro.close()
                except Exception:
                    pass
                await asyncio.sleep(30)
            else:
                return gopro, True
        except FailedToFindDevice:
            log.info("No GoPro found, retrying in 5 s...")
            await asyncio.sleep(5)
        except Exception as e:
            log.warning("COHN connection error: %s — retrying in 5 s.", e)
            await asyncio.sleep(5)


async def _race_discovery(state: AppState) -> tuple:
    """Race USB and wireless discovery; return (gopro, is_wireless) for the winner."""
    usb_task = asyncio.create_task(_discover_usb())
    cohn_task = asyncio.create_task(_discover_wireless(state))
    done, pending = await asyncio.wait([usb_task, cohn_task], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    # If both completed simultaneously, close the extra connection
    winner = None
    for t in done:
        if winner is None:
            winner = t
        else:
            try:
                extra_gopro, _ = t.result()
                await extra_gopro.close()
            except Exception:
                pass
    return winner.result()


async def _run_session(gopro, is_wireless: bool, state: AppState, today_dir: Path) -> None:
    """Execute a full transfer session with an already-connected gopro."""
    state.connection_type = "cohn" if is_wireless else "usb"
    name_resp = await gopro.http_command.get_camera_name()
    info_resp = await gopro.http_command.get_camera_info()
    state.camera_model = (info_resp.data.model_name if info_resp.ok else None)
    state.camera_name = (name_resp.data if name_resp.ok else None)
    # Use the full serial from the HTTP API so it is consistent across USB and COHN.
    # gopro.identifier returns the BLE short name (e.g. "6313") for wireless but
    # the full serial (e.g. "C3521324526313") for USB, which would break deduplication.
    state.camera_serial = (info_resp.data.serial_number if info_resp.ok else gopro.identifier)
    _update_camera_registry(state)
    push_event(state)
    _write_session_file(today_dir, state)
    # Enrich the COHN DB entry with the full serial and camera name obtained via HTTP.
    # The DB initially only stores the BLE short serial (e.g. "6313").
    if is_wireless and COHN_DB.exists() and state.camera_serial:
        import tinydb
        _db = tinydb.TinyDB(COHN_DB)
        _db.update(
            {"full_serial": state.camera_serial, "camera_name": state.camera_name},
            tinydb.Query().serial.test(lambda s: state.camera_serial.endswith(s)),
        )
        _db.close()
    log.info("Connected: %s (%s) via %s", state.camera_model, state.camera_serial,
             "COHN" if is_wireless else "USB")

    files = await get_todays_files(gopro, today_dir, state.camera_serial)
    if not files:
        log.info("No new video files from today. Nothing to do.")
        state.phase = "safe"
        push_event(state)
        return

    state.phase = "transferring"
    state.files = [FileEntry(filename=Path(item.filename).name) for item in files]
    push_event(state)
    log.info("Starting download + processing (%d file(s))...", len(files))

    queue: asyncio.Queue = asyncio.Queue()
    watchdog = _wireless_watchdog(gopro, state) if is_wireless else _usb_watchdog(state)

    # Start the processor as a background task so it can work on already-downloaded
    # files while the next download is in progress.
    processor_task = asyncio.create_task(
        run_processor(queue, today_dir, n_workers=N_PROCESS_WORKERS, state=state, today_dir=today_dir, serial=state.camera_serial)
    )
    try:
        try:
            # Phase 1: download all files (watchdog exits automatically when phase→"processing").
            await asyncio.gather(
                run_downloader(gopro, files, queue, RAW_DIR, state),
                watchdog,
            )
        except UnexpectedDisconnect:
            log.warning("Camera disconnected unexpectedly — aborting and cleaning up raw files.")
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass
            _cleanup_raw_dir()
            state.phase = "error"
            push_event(state)
            return

        # Downloads complete — camera no longer needed; disconnect now.
        try:
            await gopro.close()
            log.info("Camera disconnected after download. Processing continues in background...")
        except Exception:
            pass

        # Phase 2: wait for processing to finish (no camera connection needed).
        await processor_task
        state.phase = "safe"
        push_event(state)
        log.info("All done. Processed videos: %s", today_dir)
    except Exception:
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(state: AppState, wireless: bool) -> None:
    ensure_dirs()

    while True:
        try:
            today_dir = today_processed_dir()
            today_dir.mkdir(parents=True, exist_ok=True)

            state.phase = "connecting"
            state.connection_type = None
            state.camera_serial = None
            state.camera_name = None
            state.camera_model = None
            state.files = []
            refresh_today_stats(state, today_dir)
            push_event(state)

            if wireless:
                log.info("Scanning for GoPro over USB or COHN (Camera on Home Network)...")
                gopro, is_wireless_conn = await _race_discovery(state)
                try:
                    await _run_session(gopro, is_wireless_conn, state, today_dir)
                finally:
                    try:
                        await gopro.close()
                    except Exception:
                        pass
                    if is_wireless_conn and state.camera_serial:
                        expiry = datetime.now() + timedelta(hours=COOLDOWN_HOURS)
                        state.cooldowns[state.camera_serial] = expiry
                        log.info("Camera %s in cooldown until %s.", state.camera_serial, expiry.strftime("%H:%M"))
                if not is_wireless_conn:
                    await wait_for_usb_gopro_disconnect()
            else:
                log.info("Connecting to GoPro via USB...")
                await wait_for_usb_gopro()
                async with WiredGoPro(serial=_usb_serial_hint(), poll_period=0.5) as gopro:
                    await _run_session(gopro, False, state, today_dir)
                await wait_for_usb_gopro_disconnect()

            _cleanup_raw_dir()
            state.phase = "waiting"
            state.connection_type = None
            state.camera_serial = None
            state.camera_name = None
            state.camera_model = None
            state.files = []
            push_event(state)

            if not wireless:
                await wait_for_usb_gopro()

        except FailedToFindDevice:
            log.info("Camera not found, retrying...")
            state.phase = "waiting"
            push_event(state)
            if not wireless:
                await wait_for_usb_gopro()
        except Exception as e:
            log.error("Unexpected error: %s", e)
            state.phase = "error"
            push_event(state)
            _cleanup_raw_dir()
            if not wireless:
                await wait_for_usb_gopro_disconnect()
                await wait_for_usb_gopro()
            else:
                await asyncio.sleep(5)


async def entrypoint(args: argparse.Namespace) -> None:
    state = AppState()
    server_task = asyncio.create_task(run_web_server(state, port=8080))
    try:
        await main(state, args.wireless)
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process today's GoPro videos via USB.")
    parser.add_argument(
        "--wireless",
        action="store_true",
        help="Also discover GoPro cameras via COHN (Camera on Home Network). Uses BLE on first use to fetch credentials, then connects directly via HTTP. Requires COHN to be enabled on the camera first (via GoPro app).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging (includes open_gopro internals)",
    )
    args = parser.parse_args()
    setup_logging(args.verbose)
    asyncio.run(entrypoint(args))
