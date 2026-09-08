"""
Ro-MIDI streaming decode API  (v2 — windowed / disk-backed)

Turns any-size .mid into an on-disk, globally sorted, chunk-indexed schedule and
serves it to the game one time-window at a time, so massive black MIDIs (up to
BILLIONS of notes) can be streamed without ever holding the whole song in memory
— on the server OR in Roblox.

Big files are parsed ONCE in a background process and cached by their content
"code" (sha1 of the file). The game polls /decode until status == "ready", then
fetches /index once and pulls /chunk/<code>/<i> on demand as the playhead moves.

ZIP "folders": /decode also accepts a link to a .zip. The server unzips it and
finds the .mid/.midi inside. One MIDI -> it just decodes that one. SEVERAL MIDIs
-> it replies status == "choices" with the list of names, so the game can ask
which one; the game re-calls /decode with &pick=<index> to extract that one.

Endpoints (all the game needs are GETs, each response well under Roblox's ~1 MB):
  GET  /                       -> service info
  GET  /health                 -> {"ok": true}
  GET  /decode?url=<mid-url>    -> START (or poll) an async build; returns {status,...}
  GET  /decode?url=<zip>&pick=N -> extract MIDI #N (0-based) from a zip folder
  POST /decode                 -> body {"url": "..."} OR multipart file "file" (.mid or .zip)
  GET  /status/<job>           -> job status by job id (alt to polling /decode)
  GET  /header/<code>          -> the cached header JSON (incl. chunkCount)
  GET  /index/<code>           -> raw u32[chunkCount] little-endian: first start_ms of each chunk
  GET  /chunk/<code>/<i>       -> raw bytes of chunk i (application/octet-stream)

Status values: starting -> downloading -> queued -> parsing -> assembling -> ready
(or error, or choices). "code" appears once the source is hashed; the full header
once ready; "choices" carries a list of MIDI names when a zip holds more than one.
"""

import os
import re
import io
import sys
import json
import time
import shutil
import zipfile
import hashlib
import threading
import subprocess

import requests
from flask import Flask, request, jsonify, Response, abort

import decoder            # for FORMAT + the in-memory path (small uploads)
import store as store_mod

app = Flask(__name__)

# CACHE_DIR should point at a PERSISTENT disk on the host (Render disk mount),
# so parsed schedules survive restarts/redeploys and are reused forever.
CACHE_DIR = os.environ.get("CACHE_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache"))
TMP_DIR = os.path.join(CACHE_DIR, "_tmp")
URLMAP_DIR = os.path.join(CACHE_DIR, "_urlmap")
for _d in (CACHE_DIR, TMP_DIR, URLMAP_DIR):
    os.makedirs(_d, exist_ok=True)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_DOWNLOAD_BYTES = int(os.environ.get("MAX_DOWNLOAD_BYTES", 6 * 1024 * 1024 * 1024))  # 6 GB
FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", 180))
CODE_RE = re.compile(r"^[0-9A-Fa-f]{8}$")

_JOBS = {}                       # jobkey -> status dict
_JOBS_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()   # only one heavy build at a time (disk + CPU safety)


# ── helpers ──────────────────────────────────────────────────────────
def _store_dir(code):
    return os.path.join(CACHE_DIR, code.upper())


def _load_header(code):
    if not code:
        return None
    hp = os.path.join(_store_dir(code), "header.json")
    if os.path.exists(hp):
        try:
            with open(hp) as f:
                return json.load(f)
        except (OSError, ValueError):
            return None
    return None


def _jobkey(url):
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def _set_job(jk, **kw):
    with _JOBS_LOCK:
        j = _JOBS.get(jk) or {}
        j.update(kw)
        j["updated"] = time.time()
        _JOBS[jk] = j
        return dict(j)


def _get_job(jk):
    with _JOBS_LOCK:
        return dict(_JOBS.get(jk) or {})


def _sha1_file(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for part in iter(lambda: f.read(1 << 20), b""):
            h.update(part)
    return h.hexdigest()


_ZIP_SIGS = (b"\x03\x04", b"\x05\x06", b"\x07\x08")


def _is_zip(path):
    try:
        with open(path, "rb") as f:
            sig = f.read(4)
        return sig[:2] == b"PK" and sig[2:4] in _ZIP_SIGS
    except OSError:
        return False


def _zip_midi_names(names_source):
    """Sorted list of the .mid/.midi entries in a zipfile.ZipFile, skipping dirs and mac junk."""
    out = []
    for info in names_source.infolist():
        if info.is_dir():
            continue
        name = info.filename
        base = name.rsplit("/", 1)[-1]
        if not base or base.startswith("._") or name.startswith("__MACOSX/") or "/__MACOSX/" in name:
            continue
        if base.lower().endswith((".mid", ".midi")):
            out.append(name)
    out.sort(key=lambda s: s.lower())
    return out


def _resolve_pick(names, pick):
    """pick may be a 0-based index (as str/int) or a filename; returns the matching entry or None."""
    if pick is None:
        return None
    s = str(pick).strip()
    if s == "":
        return None
    if s.isdigit():
        idx = int(s)
        return names[idx] if 0 <= idx < len(names) else None
    for n in names:
        if n == s or n.rsplit("/", 1)[-1] == s:
            return n
    return None


def _download(url, dst, jk):
    if not re.match(r"^https?://", url, re.I):
        raise ValueError("url must be http(s)")
    h = hashlib.sha1()
    total = 0
    with requests.get(url, stream=True, timeout=FETCH_TIMEOUT,
                      headers={"User-Agent": "RoMIDI-Decoder/2"}) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for part in r.iter_content(1 << 20):
                if not part:
                    continue
                total += len(part)
                if total > MAX_DOWNLOAD_BYTES:
                    raise ValueError("source exceeds the %d-byte download limit" % MAX_DOWNLOAD_BYTES)
                h.update(part)
                f.write(part)
                _set_job(jk, status="downloading", bytes=total)
    if total == 0:
        raise ValueError("downloaded 0 bytes")
    return h.hexdigest()[:8].upper()


def _run_job(jk, url, pick=None):
    url_jk = _jobkey(url)                              # zip cache is keyed by URL (shared across picks)
    tmp = os.path.join(TMP_DIR, jk + ".src")           # the raw download (mid OR zip)
    midtmp = os.path.join(TMP_DIR, jk + ".mid")        # a MIDI extracted from a zip
    zip_cache = os.path.join(TMP_DIR, url_jk + ".zipsrc")  # kept between the "choices" reply and the pick
    keep_zip = False
    try:
        # get the source bytes — reuse a cached zip on a follow-up pick so we don't re-download it
        if pick is not None and os.path.exists(zip_cache):
            src_path, dl_code = zip_cache, None
        else:
            _set_job(jk, status="downloading", bytes=0)
            dl_code = _download(url, tmp, jk)
            src_path = tmp

        if _is_zip(src_path):
            # a "folder" of MIDIs — find them
            try:
                with zipfile.ZipFile(src_path) as z:
                    names = _zip_midi_names(z)
            except zipfile.BadZipFile:
                raise RuntimeError("that .zip is corrupt or not a real zip")
            if not names:
                raise RuntimeError("that zip has no .mid or .midi files in it")
            # keep the zip so a follow-up &pick doesn't have to download it again
            if src_path != zip_cache:
                try:
                    shutil.copyfile(src_path, zip_cache)
                except OSError:
                    pass
            chosen = _resolve_pick(names, pick)
            if chosen is None:
                if pick is not None and str(pick).strip() != "":
                    raise RuntimeError("that MIDI is not in the zip — reload the folder and pick again")
                if len(names) == 1:
                    chosen = names[0]                  # only one inside → just use it
                else:
                    bases = [n.rsplit("/", 1)[-1] for n in names]   # ask the game which one
                    _set_job(jk, status="choices", choices=bases, choiceCount=len(bases))
                    keep_zip = True
                    return
            with zipfile.ZipFile(zip_cache if os.path.exists(zip_cache) else src_path) as z:
                data = z.read(chosen)
            with open(midtmp, "wb") as f:
                f.write(data)
            build_src = midtmp
            code = _sha1_file(build_src)[:8].upper()
            _set_job(jk, status="queued", code=code, pickedName=chosen.rsplit("/", 1)[-1])
        else:
            build_src = src_path                        # a plain .mid
            code = dl_code or _sha1_file(build_src)[:8].upper()
            _set_job(jk, status="queued", code=code)

        with _BUILD_LOCK:
            hdr = _load_header(code)
            if hdr is None:
                out_dir = _store_dir(code)
                os.makedirs(out_dir, exist_ok=True)
                _set_job(jk, status="parsing", code=code, noteCount=0)
                proc = subprocess.Popen(
                    [sys.executable, os.path.join(APP_DIR, "build_worker.py"), build_src, out_dir, code],
                    cwd=APP_DIR,
                )
                pf = os.path.join(out_dir, "progress.json")
                while proc.poll() is None:
                    time.sleep(0.5)
                    try:
                        with open(pf) as f:
                            pr = json.load(f)
                        ph = pr.get("phase", "parsing")
                        _set_job(jk, status=("assembling" if ph == "ready" else ph),
                                 code=code, noteCount=max(0, pr.get("notes", 0)))
                    except (OSError, ValueError):
                        pass
                if proc.returncode != 0:
                    err = "parse failed"
                    ep = os.path.join(out_dir, "error.txt")
                    if os.path.exists(ep):
                        err = open(ep).read().strip() or err
                    raise RuntimeError(err)
                hdr = _load_header(code)
                if hdr is None:
                    raise RuntimeError("build finished but no header was written")

            try:
                with open(os.path.join(URLMAP_DIR, jk), "w") as f:
                    f.write(code)
            except OSError:
                pass

        _set_job(jk, status="ready", code=code, noteCount=hdr["noteCount"],
                 chunkCount=hdr["chunkCount"], durationMs=hdr["durationMs"],
                 trackCount=hdr["trackCount"])
    except Exception as e:  # noqa: BLE001
        _set_job(jk, status="error", err=str(e))
    finally:
        for p in (tmp, midtmp):
            try:
                os.remove(p)
            except OSError:
                pass
        if not keep_zip:                                # only drop the zip once we're past the pick
            try:
                os.remove(zip_cache)
            except OSError:
                pass


def _ready_payload(code):
    hdr = _load_header(code) or {}
    return {"ok": True, "status": "ready", **hdr}


# ── routes ───────────────────────────────────────────────────────────
@app.route("/decode", methods=["GET", "POST"])
def decode():
    # multipart upload (e.g. RAMP) — build synchronously (uploads are the user's own,
    # usually modest; still cached by code). A .zip upload is unpacked the same way.
    if request.method == "POST" and request.files.get("file"):
        data = request.files["file"].read()
        if not data:
            abort(400, "empty file")
        pick = request.form.get("pick")
        if data[:2] == b"PK" and data[2:4] in _ZIP_SIGS:
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    names = _zip_midi_names(z)
                    if not names:
                        abort(400, "that zip has no .mid or .midi files in it")
                    chosen = _resolve_pick(names, pick)
                    if chosen is None:
                        if pick is not None and str(pick).strip() != "":
                            abort(400, "picked MIDI is not in the zip")
                        if len(names) == 1:
                            chosen = names[0]
                        else:
                            return jsonify({"ok": True, "status": "choices",
                                            "choices": [n.rsplit("/", 1)[-1] for n in names],
                                            "choiceCount": len(names)})
                    data = z.read(chosen)
            except zipfile.BadZipFile:
                abort(400, "that .zip is corrupt or not a real zip")
        code = hashlib.sha1(data).hexdigest()[:8].upper()
        if _load_header(code) is None:
            tmp = os.path.join(TMP_DIR, code + ".upload.mid")
            with open(tmp, "wb") as f:
                f.write(data)
            try:
                with _BUILD_LOCK:
                    if _load_header(code) is None:
                        store_mod.build_store(tmp, _store_dir(code), code)
            finally:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
        return jsonify(_ready_payload(code))

    url = request.args.get("url") or (request.is_json and (request.get_json(silent=True) or {}).get("url"))
    if not url:
        abort(400, "provide ?url=<mid|zip>, a JSON body {\"url\":...}, or a multipart 'file'")
    url = url.strip()

    pick = request.args.get("pick")
    if pick is None and request.is_json:
        pick = (request.get_json(silent=True) or {}).get("pick")
    if pick is not None and str(pick).strip() == "":
        pick = None
    # a pick is its own job (same zip, different chosen MIDI) so it never collides with the choices job
    jk = _jobkey(url + ("\x00PICK\x00" + str(pick) if pick is not None else ""))
    j = _get_job(jk)

    if j.get("status") == "ready" and _load_header(j.get("code")):
        return jsonify(_ready_payload(j["code"]))

    if j.get("status") == "choices":
        return jsonify({"ok": True, "status": "choices", "job": jk,
                        "choices": j.get("choices", []), "choiceCount": j.get("choiceCount", 0)})

    # resume a cached result by URL after a restart (in-memory job map is gone,
    # but the store on the persistent disk isn't)
    if not j:
        mp = os.path.join(URLMAP_DIR, jk)
        if os.path.exists(mp):
            try:
                code = open(mp).read().strip()
            except OSError:
                code = ""
            hdr = _load_header(code)
            if hdr:
                _set_job(jk, status="ready", code=code, noteCount=hdr["noteCount"],
                         chunkCount=hdr["chunkCount"], durationMs=hdr["durationMs"],
                         trackCount=hdr["trackCount"])
                return jsonify(_ready_payload(code))

    if j.get("status") in ("starting", "downloading", "queued", "parsing", "assembling"):
        return jsonify({"ok": True, "status": j["status"], "job": jk,
                        "bytes": j.get("bytes", 0), "noteCount": j.get("noteCount", 0),
                        "code": j.get("code")})

    if j.get("status") == "error":
        # allow a retry by clearing and restarting
        pass

    _set_job(jk, status="starting", bytes=0, noteCount=0)
    threading.Thread(target=_run_job, args=(jk, url, pick), daemon=True).start()
    return jsonify({"ok": True, "status": "starting", "job": jk})


@app.get("/status/<job>")
def status(job):
    j = _get_job(job)
    if not j:
        return jsonify({"ok": False, "status": "unknown"}), 404
    out = {"ok": True, "status": j.get("status"), "bytes": j.get("bytes", 0),
           "noteCount": j.get("noteCount", 0)}
    if j.get("code"):
        out["code"] = j["code"]
    if j.get("status") == "choices":
        out["choices"] = j.get("choices", [])
        out["choiceCount"] = j.get("choiceCount", 0)
    if j.get("status") == "ready":
        out.update(_load_header(j["code"]) or {})
    if j.get("status") == "error":
        out["err"] = j.get("err")
    return jsonify(out)


@app.get("/header/<code>")
def header(code):
    if not CODE_RE.match(code):
        abort(400, "bad code")
    hdr = _load_header(code.upper())
    if not hdr:
        abort(404, "unknown code")
    return jsonify({"ok": True, **hdr})


@app.get("/index/<code>")
def index_route(code):
    if not CODE_RE.match(code):
        abort(400, "bad code")
    p = os.path.join(_store_dir(code.upper()), "index.bin")
    if not os.path.exists(p):
        abort(404, "unknown code")
    with open(p, "rb") as f:
        data = f.read()
    return Response(data, mimetype="application/octet-stream")


@app.get("/chunk/<code>/<int:i>")
def chunk(code, i):
    if not CODE_RE.match(code):
        abort(400, "bad code")
    hdr = _load_header(code.upper())
    if not hdr:
        abort(404, "unknown code")
    step = hdr["notesPerChunk"] * hdr["recordBytes"]
    p = os.path.join(_store_dir(code.upper()), "sched.bin")
    if not os.path.exists(p):
        abort(404, "no schedule")
    with open(p, "rb") as f:
        f.seek(i * step)
        blob = f.read(step)
    return Response(blob, mimetype="application/octet-stream",
                    headers={"X-Chunk": str(i), "X-Chunk-Count": str(hdr["chunkCount"])})


@app.get("/")
def info():
    return jsonify({
        "service": "Ro-MIDI streaming decode API",
        "format": decoder.FORMAT,
        "flow": "GET /decode?url=<mid|zip> (poll until status=ready; a multi-MIDI zip returns status=choices — re-call with &pick=<index>) -> GET /index/<code> -> GET /chunk/<code>/<i>",
    })


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), threaded=True)
