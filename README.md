# Ro-MIDI Decode API — v2 (streaming / windowed)

Turns **any-size** `.mid` into an on-disk, globally sorted, chunk-indexed schedule
and streams it to the Roblox game **one time-window at a time**. This is what lets
Ro-MIDI load massive black MIDIs (target: up to ~500M notes) without ever holding
the whole song in memory — on the server *or* in Roblox.

The heavy parse happens **once**, in a background process, and is cached by the
file's content **code** (`sha1` of the bytes). After that the same file loads
instantly, forever, for everyone.

## Why it's built this way

- A single Roblox HTTP response tops out ~1 MB, so notes are packed into **9 bytes**
  each and served in **60,000-note chunks** (540 KB) that the game pulls on demand.
- The parser uses **flat memory**: it mmaps the file, spills notes to per-time-bucket
  files on disk, then assembles a sorted schedule bucket-by-bucket. Peak RAM stays
  ~**under 300 MB even for hundreds of millions of notes** — the real requirement is
  **disk**, not RAM.
- The game never loads the whole schedule. It reads the small **chunk index** (first
  start-ms of each chunk) and fetches only the chunks near the playhead as the song
  plays, dropping ones that have passed.

## Endpoints (the game only needs GETs; every response is well under 1 MB)

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | service info |
| GET | `/health` | `{"ok": true}` |
| GET | `/decode?url=<mid-url>` | **start or poll** an async build → `{status, ...}` |
| POST | `/decode` | body `{"url": "..."}` **or** multipart `file` (RAMP uploads) |
| GET | `/status/<job>` | job status by job id (alternative to polling `/decode`) |
| GET | `/header/<code>` | cached header JSON |
| GET | `/index/<code>` | raw `u32[chunkCount]` little-endian: first start-ms of each chunk |
| GET | `/chunk/<code>/<i>` | raw bytes of chunk `i` (`0 .. chunkCount-1`) |

**Flow:** `GET /decode?url=…` and poll until `status == "ready"` → read `code` +
`chunkCount` from the reply → `GET /index/<code>` once → `GET /chunk/<code>/<i>` on
demand while playing.

**Status values:** `starting → downloading → queued → parsing → assembling → ready`
(or `error`). The reply carries `bytes` while downloading and `noteCount` while
parsing, so the game can show progress.

**Header JSON**
```json
{ "ok": true, "code": "63B97318", "format": "romidi-sched-v1",
  "noteCount": 129000000, "durationMs": 895833, "trackCount": 40,
  "recordBytes": 9, "notesPerChunk": 60000, "chunkCount": 2150 }
```

## Wire format `romidi-sched-v1`

Flat array of fixed **9-byte little-endian** records, sorted by start:

```
< u32 start_ms >< u16 dur_ms >< u8 midi >< u8 track >< u8 velocity >
```

`dur_ms` clamps to 1..65535; `track` clamps to 0..255. Roblox reads a chunk with
`buffer.fromstring(body)` then `buffer.readu32/readu16/readu8` — no JSON, no base64.

## Deploy on Render (paid tier + a persistent disk)

Two things this needs that the free tier can't give: a **persistent disk** (parsed
schedules are big and must survive restarts) and **always-on** (no spin-down that
would kill a long parse). RAM can be modest — the pipeline is memory-bounded.

1. Push this folder to a GitHub repo, create a **Web Service** from it. It auto-detects
   the `Dockerfile`.
2. Pick a **paid instance** (e.g. Starter). More CPU makes the one-time parse faster;
   RAM needs are low.
3. Add a **Disk**: mount path **`/data`**, size it for your biggest files —
   roughly **`noteCount × 9 bytes`** for the schedule plus about the same again as
   transient scratch during a build. Rules of thumb: ~1 GB per ~50M notes for the
   final schedule; provision ~**2×** that headroom. For 500M notes, ~10 GB.
   The Dockerfile already points `CACHE_DIR` at `/data`.
4. Env vars (optional — Dockerfile sets sensible defaults):
   - `CACHE_DIR=/data` (must match the disk mount)
   - `MALLOC_ARENA_MAX=2` (keeps RSS tidy)
   - `MAX_DOWNLOAD_BYTES` (default 6 GB) — max source file size to fetch
   - `MAX_NOTES` (default 600,000,000) — hard note ceiling
5. **Health check path:** `/health`.

Then in Roblox: **Game Settings → Security → Allow HTTP Requests = ON**, and point
`RoMIDI_DecodeProxy` at your service URL.

### First load of a brand-new huge file is slow (once)

Parsing is ~**2–3 seconds per million notes** (e.g. ~1 second for a normal song;
several minutes for a 100M+ monster). It runs in the background and is cached by
`code`, so the **next** load of that file — for anyone — is instant.

## Run locally

```bash
pip install -r requirements.txt
python app.py            # http://localhost:8080  (CACHE_DIR defaults to ./cache)
curl "http://localhost:8080/decode?url=https://example.com/song.mid"   # poll until status=ready
```

## Files

- `app.py` — the web service (async jobs, caching, windowed serving).
- `store.py` — the memory-bounded, disk-backed store builder (mmap + bucketed sort).
- `build_worker.py` — runs a build in a separate process (keeps the web service responsive).
- `decoder.py` — the shared byte-level MIDI parser + tempo/format helpers (also a fast
  in-memory path used for small multipart uploads and tests).
