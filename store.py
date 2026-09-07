"""
Ro-MIDI streaming STORE builder — turns any-size .mid into an on-disk, globally
sorted, chunk-indexed schedule using flat (bounded) memory, so massive black
MIDIs (up to hundreds of millions of notes) can be built on modest hardware and
then STREAMED to the game a time-window at a time.

Pipeline (RAM stays flat regardless of file size):
  1. mmap the source .mid (never read the whole file into RAM).
  2. Parse note events; spill each note as an 11-byte tick-record into a
     per-time-bucket file on disk (a small LRU of open handles). Collect tempo
     events (tiny).
  3. Assemble: walk the bucket files in time order; for each bucket convert
     ticks->ms with numpy, sort within the bucket, append 9-byte records to the
     final schedule (sched.bin). Buckets in ascending time order => the whole
     file is globally sorted by start time.
  4. Build a chunk index (first start-ms of every notesPerChunk records) so the
     game knows which chunk covers which moment and can fetch on demand.

Output files in out_dir:
  sched.bin    — the packed schedule: N * 9-byte records (romidi-sched-v1)
  index.bin    — chunkCount * u32 little-endian: first start_ms of each chunk
  header.json  — {format,noteCount,durationMs,trackCount,recordBytes,notesPerChunk,chunkCount,code,trackNames,trackNotes}

sched.bin is the SAME 9-byte record format the game already reads, so a "chunk"
is just sched.bin[i*notesPerChunk*9 : (i+1)*notesPerChunk*9].
"""

import os
import json
import struct
import mmap
from collections import OrderedDict

import numpy as np

from decoder import _build_tempo_map, _ticks_to_seconds, REC_BYTES, FORMAT, DEFAULT_NOTES_PER_CHUNK

SPILL = struct.Struct("<IIBBB")     # start_tick, end_tick, midi, track, vel  (11 bytes)
SPILL_BYTES = SPILL.size

# hard ceiling so a hostile/insane file can't fill the disk; env-overridable on a big instance
MAX_NOTES = int(os.environ.get("MAX_NOTES", 600_000_000))   # 600M * 9 = ~5.4 GB on disk
_PROGRESS_EVERY = 2_000_000


def _read_vlq(mv, pos):
    v = 0
    while True:
        b = mv[pos]
        pos += 1
        v = (v << 7) | (b & 0x7F)
        if not (b & 0x80):
            return v, pos


class _Buckets:
    """Append-only writers for time-bucket spill files, with a bounded set of
    open file handles (LRU) so a very long song can't blow the fd limit."""

    def __init__(self, d, max_open=200):
        self.d = d
        self.max_open = max_open
        self.handles = OrderedDict()   # bucket_idx -> open file

    def _path(self, b):
        return os.path.join(self.d, "b%010d.tmp" % b)

    def write(self, b, data):
        h = self.handles.get(b)
        if h is None:
            if len(self.handles) >= self.max_open:
                _, old = self.handles.popitem(last=False)
                old.close()
            h = open(self._path(b), "ab", buffering=1 << 16)
            self.handles[b] = h
        else:
            self.handles.move_to_end(b)
        h.write(data)

    def close(self):
        for h in self.handles.values():
            h.close()
        self.handles.clear()


def build_store(src_path, out_dir, code, bucket_beats=4, progress=None):
    """Build the on-disk store for the .mid at src_path into out_dir. Returns the header dict."""
    os.makedirs(out_dir, exist_ok=True)
    tmp = os.path.join(out_dir, "_buckets")
    os.makedirs(tmp, exist_ok=True)

    with open(src_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    mv = memoryview(mm)
    n = len(mv)
    try:
        if n < 14 or bytes(mv[0:4]) != b"MThd":
            raise ValueError("not a MIDI file (missing MThd header)")
        _, _, division = struct.unpack(">HHH", bytes(mv[8:14]))
        if division & 0x8000:
            frames = 256 - (division >> 8)
            subframes = division & 0xFF
            tps = max(1, frames * subframes)
            smpte, tpb = True, 1
        else:
            smpte, tps = False, 1
            tpb = division or 480
        bucket_ticks = max(1, tpb * bucket_beats)

        buckets = _Buckets(tmp)
        pack = SPILL.pack
        tempo_ticks, tempo_vals = [], []
        note_total = 0
        last_prog = 0
        track_index = -1
        track_names = {}   # track_index -> name (from FF 03 meta)
        track_notes = {}   # track byte (0..255) -> note count

        pos = 0
        while pos + 8 <= n:
            cid = bytes(mv[pos:pos + 4])
            length = int.from_bytes(mv[pos + 4:pos + 8], "big")
            body_start = pos + 8
            body_end = min(n, body_start + length)
            pos = body_start + length
            if cid != b"MTrk":
                continue
            track_index += 1
            tclamp = track_index if track_index < 255 else 255
            p = body_start
            abs_tick = 0
            status = 0
            active = {}
            while p < body_end:
                delta, p = _read_vlq(mv, p)
                abs_tick += delta
                if p >= body_end:
                    break
                b = mv[p]
                if b & 0x80:
                    status = b
                    p += 1
                    if b == 0xFF:
                        mtype = mv[p]
                        p += 1
                        length2, p = _read_vlq(mv, p)
                        if mtype == 0x51 and length2 == 3:
                            tempo_ticks.append(abs_tick)
                            tempo_vals.append((mv[p] << 16) | (mv[p + 1] << 8) | mv[p + 2])
                            p += length2
                        elif mtype == 0x03:
                            seg = bytes(mv[p:p + length2])
                            p += length2
                            if track_index not in track_names:
                                nm = seg.decode("utf-8", "ignore").replace("\x00", "").strip()
                                if nm:
                                    track_names[track_index] = nm[:48]
                        elif mtype == 0x2F:
                            p += length2
                            break
                        else:
                            p += length2
                        status = 0
                        continue
                    if b == 0xF0 or b == 0xF7:
                        length2, p = _read_vlq(mv, p)
                        p += length2
                        status = 0
                        continue
                else:
                    if status == 0 or status >= 0xF0:
                        p += 1
                        continue
                hi = status & 0xF0
                chan = status & 0x0F
                if hi == 0x90:
                    note = mv[p]; vel = mv[p + 1]; p += 2
                    if vel > 0:
                        active[(chan, note)] = (abs_tick, vel)
                    else:
                        onv = active.pop((chan, note), None)
                        if onv is not None:
                            buckets.write(onv[0] // bucket_ticks,
                                          pack(onv[0], abs_tick, note & 0x7F, tclamp, onv[1] if onv[1] <= 127 else 127))
                            note_total += 1
                            track_notes[tclamp] = track_notes.get(tclamp, 0) + 1
                elif hi == 0x80:
                    note = mv[p]; p += 2
                    onv = active.pop((chan, note), None)
                    if onv is not None:
                        buckets.write(onv[0] // bucket_ticks,
                                      pack(onv[0], abs_tick, note & 0x7F, tclamp, onv[1] if onv[1] <= 127 else 127))
                        note_total += 1
                        track_notes[tclamp] = track_notes.get(tclamp, 0) + 1
                elif hi == 0xA0 or hi == 0xB0 or hi == 0xE0:
                    p += 2
                else:
                    p += 1
                if note_total - last_prog >= _PROGRESS_EVERY:
                    last_prog = note_total
                    if progress:
                        progress("parsing", note_total)
                    if note_total > MAX_NOTES:
                        raise ValueError("MIDI too large (over %d notes)" % MAX_NOTES)
            if active:
                for (chan, note), (start_tick, vel) in active.items():
                    buckets.write(start_tick // bucket_ticks,
                                  pack(start_tick, abs_tick, note & 0x7F, tclamp, vel if vel <= 127 else 127))
                    note_total += 1
                    track_notes[tclamp] = track_notes.get(tclamp, 0) + 1
        buckets.close()
    finally:
        mv.release()
        mm.close()

    track_count = track_index + 1 if track_index >= 0 else 0
    bt, cum, tp = _build_tempo_map(tempo_ticks, tempo_vals, tpb)

    final_path = os.path.join(out_dir, "sched.bin")
    dt = np.dtype([("s", "<u4"), ("d", "<u2"), ("m", "u1"), ("t", "u1"), ("v", "u1")])
    spill_dt = np.dtype([("st", "<u4"), ("et", "<u4"), ("m", "u1"), ("t", "u1"), ("v", "u1")])
    assert dt.itemsize == REC_BYTES and spill_dt.itemsize == SPILL_BYTES

    max_end_s = 0.0
    bucket_idxs = sorted(int(fn[1:-4]) for fn in os.listdir(tmp)
                         if fn.startswith("b") and fn.endswith(".tmp"))
    with open(final_path, "wb") as fout:
        for bi in bucket_idxs:
            bp = os.path.join(tmp, "b%010d.tmp" % bi)
            raw = np.fromfile(bp, dtype=spill_dt)
            os.remove(bp)
            if len(raw) == 0:
                continue
            st = raw["st"].astype(np.int64)
            et = raw["et"].astype(np.int64)
            ss = _ticks_to_seconds(st, bt, cum, tp, tpb, smpte, tps)
            ee = _ticks_to_seconds(et, bt, cum, tp, tpb, smpte, tps)
            out = np.empty(len(raw), dtype=dt)
            sm = np.floor(ss * 1000.0 + 0.5); np.clip(sm, 0, 0xFFFFFFFF, out=sm)
            out["s"] = sm.astype(np.uint32)
            dd = np.floor((ee - ss) * 1000.0 + 0.5); np.clip(dd, 1, 65535, out=dd)
            out["d"] = dd.astype(np.uint16)
            out["m"] = raw["m"]; out["t"] = raw["t"]; out["v"] = raw["v"]
            out.sort(order="s", kind="stable")
            fout.write(out.tobytes())
            m = float(ee.max())
            if m > max_end_s:
                max_end_s = m
    try:
        os.rmdir(tmp)
    except OSError:
        pass

    chunk_count = (note_total + DEFAULT_NOTES_PER_CHUNK - 1) // DEFAULT_NOTES_PER_CHUNK
    index = np.zeros(max(chunk_count, 0), dtype="<u4")
    if chunk_count:
        with open(final_path, "rb") as fin:
            step = DEFAULT_NOTES_PER_CHUNK * REC_BYTES
            for i in range(chunk_count):
                fin.seek(i * step)
                b4 = fin.read(4)
                if len(b4) == 4:
                    index[i] = struct.unpack("<I", b4)[0]
    index.tofile(os.path.join(out_dir, "index.bin"))

    _ntr = min(track_count, 512)
    track_names_list = [track_names.get(i, "") for i in range(_ntr)]
    track_notes_list = [int(track_notes.get(i, 0)) for i in range(_ntr)]
    header = {
        "format": FORMAT, "noteCount": note_total,
        "durationMs": int(max_end_s * 1000 + 0.5),
        "trackCount": track_count, "recordBytes": REC_BYTES,
        "notesPerChunk": DEFAULT_NOTES_PER_CHUNK, "chunkCount": chunk_count, "code": code,
        "trackNames": track_names_list, "trackNotes": track_notes_list,
    }
    with open(os.path.join(out_dir, "header.json"), "w") as f:
        json.dump(header, f)
    if progress:
        progress("ready", note_total)
    return header
