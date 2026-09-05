# Ro-MIDI Decode API

Turns big MIDIs into a **compact, chunked note schedule** so Roblox can stream them
in under its ~1 MB-per-HTTP-request limit. The heavy parsing happens here (Python +
`mido`), not in Luau, and the result is served in small binary pages the game pulls
one at a time.

## Why

- A single Roblox HTTP call tops out around **~1 MB** (`PostAsync` hard-caps at 999 KB;
  big `GetAsync` responses choke the underlying library / spike memory).
- A multi-MB MIDI can't arrive in one lump, and parsing millions of events in Luau
  freezes the client.
- This service parses once, packs each note into **9 bytes**, and serves the schedule
  in **540 KB chunks** — so no transfer ever crosses the limit.

## Endpoints (the game only needs GETs)

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | service info |
| GET | `/health` | `{"ok": true}` |
| GET | `/decode?url=<mid-url>` | fetch + parse (or cache hit) → header JSON |
| POST | `/decode` | body `{"url": "..."}` **or** multipart `file` (for RAMP uploads) |
| GET | `/header/<code>` | the cached header JSON |
| GET | `/chunk/<code>/<i>` | raw bytes of chunk `i` (`0 .. chunkCount-1`) |

**Header JSON**
```json
{ "ok": true, "code": "63B97318", "format": "romidi-sched-v1",
  "noteCount": 129000, "durationMs": 895833, "trackCount": 3,
  "recordBytes": 9, "notesPerChunk": 60000, "chunkCount": 3 }
```

`code` = first 8 hex chars of `sha1(file-bytes)`, upper-cased — deterministic, so the
same MIDI always gets the same code (dedup + room file-sync for free).

## Wire format `romidi-sched-v1`

The schedule is a flat array of fixed **9-byte little-endian** records, sorted by start:

```
< u32 start_ms >< u16 dur_ms >< u8 midi >< u8 track >< u8 velocity >
```

`dur_ms` clamps to 65535; `track` clamps to 255. Roblox reads a chunk with
`buffer.fromstring(body)` then `buffer.readu32/readu16/readu8` — no JSON, no base64.
Each note in Luau becomes `{ s = start_ms/1000, e = (start_ms+dur_ms)/1000, m = midi, tk = track+1, v = velocity }`.

## How the game uses it

1. `GET /decode?url=<midi>` → header (note count, chunk count, code).
2. Loop `GET /chunk/<code>/<i>` for `i = 0..chunkCount-1`, appending notes to the
   schedule; start playing after chunk 0 and keep streaming the rest.
3. For a **room**, everyone loads the same `code` → each client streams the same
   chunks → synced playback.

> Roblox HTTP is **server-side only**, so the game *server* makes these calls and
> hands the bytes to the client(s) via a RemoteFunction (also <1 MB per reply, which
> the chunking already guarantees).

## Run locally

```bash
pip install -r requirements.txt
python app.py           # http://localhost:8080
# test:
curl "http://localhost:8080/decode?url=https://example.com/song.mid"
```

## Deploy (pick one)

**Render / Railway (easiest)** — push this folder to a GitHub repo, create a new
Web Service from it. Both auto-detect either the `Dockerfile` or the `Procfile`.
No build command needed; start command (if asked): `gunicorn -w 2 -t 120 -b 0.0.0.0:$PORT app:app`.

**Fly.io** — `fly launch` (uses the Dockerfile), then `fly deploy`.

**Any VPS (Docker)** — `docker build -t romidi-api . && docker run -p 8080:8080 romidi-api`.

**Any VPS (bare)** — `pip install -r requirements.txt` then run the Procfile's command
behind nginx.

Then in Roblox: **Game Settings → Security → Allow HTTP Requests = ON**, and point the
loader at `https://<your-host>`.

### Note on caching / disk

Parsed schedules are cached to `./cache/`. On hosts with **ephemeral disk** (Render/
Railway free tiers) that cache is wiped on restart/redeploy — the next `/decode?url=`
just re-parses (deterministic, so same code). If you want codes to survive restarts
(e.g. code-only file-sync long after decode), attach a **persistent disk/volume** and
point `CACHE_DIR` at it.
