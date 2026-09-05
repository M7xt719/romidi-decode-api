"""
Ro-MIDI decode API  —  turns big MIDIs into a compact, chunked note schedule
so Roblox can stream them in under its ~1 MB-per-request HTTP limit.

Endpoints (all the game needs are GETs):
  GET  /                      -> service info
  GET  /health                -> {"ok": true}
  GET  /decode?url=<mid url>  -> parse (or cache hit) -> header JSON  (incl. "code")
  POST /decode                -> body {"url": "..."}  OR  multipart file field "file"
  GET  /header/<code>         -> the cached header JSON
  GET  /chunk/<code>/<i>      -> raw bytes of chunk i (application/octet-stream)

A "code" is the first 8 hex chars of sha1(file-bytes), upper-cased — deterministic,
so the same MIDI always gets the same code (dedup + room file-sync for free).

Wire format is documented in decoder.py (romidi-sched-v1).
"""

import os
import re
import json
import hashlib

import requests
from flask import Flask, request, jsonify, Response, abort

import decoder

app = Flask(__name__)

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024      # 64 MB cap on the source MIDI download
FETCH_TIMEOUT      = 25                     # seconds
CODE_RE            = re.compile(r"^[0-9A-F]{8}$")


# ── cache helpers ────────────────────────────────────────────────────
def _paths(code):
    return (os.path.join(CACHE_DIR, code + ".bin"),
            os.path.join(CACHE_DIR, code + ".json"))


def _load_header(code):
    _, hp = _paths(code)
    if not os.path.exists(hp):
        return None
    with open(hp, "r") as f:
        return json.load(f)


def _store(code, header, packed, src=None):
    bp, hp = _paths(code)
    with open(bp, "wb") as f:
        f.write(packed)
    header = dict(header)
    header["code"] = code
    if src:
        header["source"] = src
    with open(hp, "w") as f:
        json.dump(header, f)
    return header


def _code_for(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()[:8].upper()


# ── source fetching (with a size cap + basic SSRF guard) ─────────────
def _fetch(url: str) -> bytes:
    if not re.match(r"^https?://", url, re.I):
        abort(400, "url must be http(s)")
    try:
        with requests.get(url, stream=True, timeout=FETCH_TIMEOUT,
                          headers={"User-Agent": "RoMIDI-Decoder/1"}) as r:
            r.raise_for_status()
            buf = bytearray()
            for part in r.iter_content(64 * 1024):
                buf += part
                if len(buf) > MAX_DOWNLOAD_BYTES:
                    abort(413, "source MIDI exceeds %d bytes" % MAX_DOWNLOAD_BYTES)
            return bytes(buf)
    except requests.RequestException as e:
        abort(502, "could not fetch source: %s" % e)


def _decode_and_cache(data: bytes, src=None):
    code = _code_for(data)
    existing = _load_header(code)
    if existing:                      # already parsed this exact file
        return existing
    try:
        header, packed = decoder.parse_midi(data)
    except ValueError as e:
        abort(422, str(e))
    except Exception as e:
        abort(422, "not a valid MIDI: %s" % e)
    return _store(code, header, packed, src)


# ── routes ───────────────────────────────────────────────────────────
@app.get("/")
def index():
    return jsonify({
        "service": "Ro-MIDI decode API",
        "format": decoder.FORMAT,
        "usage": {
            "decode": "GET /decode?url=<mid-url>  or  POST /decode (json {url} | file)",
            "header": "GET /header/<code>",
            "chunk":  "GET /chunk/<code>/<i>  (raw bytes, i = 0..chunkCount-1)",
        },
    })


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.route("/decode", methods=["GET", "POST"])
def decode():
    data = None
    src = None
    if request.method == "POST" and request.files.get("file"):
        data = request.files["file"].read()
        src = "upload:" + (request.files["file"].filename or "file")
    else:
        url = request.args.get("url") or (request.is_json and (request.get_json(silent=True) or {}).get("url"))
        if not url:
            abort(400, "provide ?url=, a json body {\"url\":...}, or a multipart 'file'")
        src = url
        data = _fetch(url)
    if not data:
        abort(400, "no MIDI data")
    header = _decode_and_cache(data, src)
    return jsonify({"ok": True, **header})


@app.get("/header/<code>")
def header(code):
    code = code.upper()
    if not CODE_RE.match(code):
        abort(400, "bad code")
    h = _load_header(code)
    if not h:
        abort(404, "unknown code")
    return jsonify({"ok": True, **h})


@app.get("/chunk/<code>/<int:i>")
def chunk(code, i):
    code = code.upper()
    if not CODE_RE.match(code):
        abort(400, "bad code")
    h = _load_header(code)
    if not h:
        abort(404, "unknown code")
    bp, _ = _paths(code)
    step = h["notesPerChunk"] * h["recordBytes"]
    start = i * step
    with open(bp, "rb") as f:
        f.seek(start)
        blob = f.read(step)
    return Response(blob, mimetype="application/octet-stream",
                    headers={"X-Chunk": str(i), "X-Chunk-Count": str(h["chunkCount"])})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
