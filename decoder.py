"""
Ro-MIDI decode core — parse a .mid into a compact, chunkable note schedule.

MEMORY-LIGHT rewrite (no mido):
  A lean byte-level MIDI reader collects notes into compact `array`/numpy
  buffers (11 bytes/note while parsing) instead of millions of Python objects,
  and converts ticks->ms + sorts with numpy in bounded blocks. This lets huge
  "black MIDIs" (tens of MB, millions of notes) parse inside a 512 MB free-tier
  instance, where the old mido-based parser would run out of memory.

  The OUTPUT is the exact same wire format as before (romidi-sched-v1), so the
  Roblox side (DecodeProxy + MidiPlayer + Visuals) needs NO changes.

Wire format  (format id: "romidi-sched-v1")
-------------------------------------------
Flat array of fixed 9-byte little-endian records, sorted by start time:

    <  I  H  B  B  B  >
       |  |  |  |  |
       |  |  |  |  +-- velocity   (u8,  0..127)
       |  |  |  +----- track      (u8,  0..255, clamped)
       |  |  +-------- midi note  (u8,  0..127)
       |  +----------- duration   (u16, milliseconds, 1..65535 clamped)
       +-------------- start      (u32, milliseconds from song start)

Roblox reads a chunk with buffer.fromstring + buffer.readu32/readu16/readu8
(no JSON, no base64).
"""

import os
import struct
from array import array

import numpy as np

REC = struct.Struct("<IHBBB")   # 9 bytes / note
REC_BYTES = REC.size            # 9
FORMAT = "romidi-sched-v1"

# safety caps (env-overridable so a bigger paid instance can raise them).
# 10M notes peaks ~415 MB RAM here — safe under a 512 MB free instance. Raise
# MAX_NOTES only if the instance has more RAM.
MAX_NOTES = int(os.environ.get("MAX_NOTES", 10_000_000))
DEFAULT_NOTES_PER_CHUNK = int(os.environ.get("NOTES_PER_CHUNK", 60_000))  # 60000*9 = 540 KB/chunk
_CONV_BLOCK = 1_000_000          # notes per block for the tick->ms / sort passes


# ── low-level MIDI reading ───────────────────────────────────────────
def _read_vlq(data, pos):
    """Read a MIDI variable-length quantity. Returns (value, new_pos)."""
    value = 0
    while True:
        b = data[pos]
        pos += 1
        value = (value << 7) | (b & 0x7F)
        if not (b & 0x80):
            return value, pos


def _iter_chunks(data):
    """Yield (chunk_id_bytes, body_bytes) for each top-level chunk."""
    pos, n = 0, len(data)
    while pos + 8 <= n:
        cid = data[pos:pos + 4]
        length = int.from_bytes(data[pos + 4:pos + 8], "big")
        body = data[pos + 8:pos + 8 + length]
        yield cid, body
        pos += 8 + length


# ── tempo map ────────────────────────────────────────────────────────
def _build_tempo_map(tempo_ticks, tempo_vals, tpb):
    """(ticks[], microsec-per-quarter[]) -> (boundary_ticks np.int64, cum_seconds np.float64, tempos np.float64).

    Mirrors the previous mido-based behaviour: sort by tick, last tempo at a
    given tick wins, and a (tick=0, 500000 = 120 BPM) boundary is guaranteed.
    """
    pairs = sorted(zip(tempo_ticks, tempo_vals), key=lambda x: x[0])
    bt, tp = [], []
    for tick, tempo in pairs:
        if bt and bt[-1] == tick:
            tp[-1] = tempo
        else:
            bt.append(tick)
            tp.append(tempo)
    if not bt or bt[0] != 0:
        bt.insert(0, 0)
        tp.insert(0, 500000)

    bt = np.asarray(bt, dtype=np.int64)
    tp = np.asarray(tp, dtype=np.float64)
    # cumulative seconds at each boundary
    seg = np.zeros(len(bt), dtype=np.float64)
    if len(bt) > 1:
        seg[1:] = np.diff(bt) / tpb * (tp[:-1] / 1_000_000.0)
    cum = np.cumsum(seg)
    return bt, cum, tp


def _ticks_to_seconds(ticks_i64, bt, cum, tp, tpb, smpte, tps):
    """Vectorised tick->second for one block (ticks_i64 already int64)."""
    if smpte:
        return ticks_i64 / float(tps)
    idx = np.searchsorted(bt, ticks_i64, side="right") - 1
    np.clip(idx, 0, len(bt) - 1, out=idx)
    return cum[idx] + (ticks_i64 - bt[idx]) / tpb * (tp[idx] / 1_000_000.0)


# ── main parse ───────────────────────────────────────────────────────
def parse_midi(data: bytes):
    """Parse raw .mid bytes -> (header_dict, packed_bytes). Memory-light."""
    if len(data) < 14 or data[:4] != b"MThd":
        raise ValueError("not a MIDI file (missing MThd header)")

    fmt, ntrks, division = struct.unpack(">HHH", data[8:14])
    if division & 0x8000:                         # SMPTE time division (rare)
        frames = 256 - (division >> 8)            # negative signed byte
        subframes = division & 0xFF
        tps = max(1, frames * subframes)          # ticks per second
        smpte, tpb = True, 1
    else:
        smpte, tps = False, 1
        tpb = division or 480

    # compact per-note buffers (int32 ticks are safe: 2^31 ticks is ~weeks of music)
    st = array("i")   # start abs-tick
    et = array("i")   # end abs-tick
    mi = array("B")   # midi note
    tk = array("B")   # track (clamped 0..255)
    ve = array("B")   # velocity
    st_a, et_a = st.append, et.append
    mi_a, tk_a, ve_a = mi.append, tk.append, ve.append
    tempo_ticks, tempo_vals = [], []

    track_index = -1
    note_total = 0

    for cid, body in _iter_chunks(data):
        if cid != b"MTrk":
            continue
        track_index += 1
        tclamp = track_index if track_index < 255 else 255
        pos, blen = 0, len(body)
        abs_tick = 0
        status = 0
        active = {}   # (chan,note) -> (start_tick, vel)  [single value; matches prior behaviour]
        while pos < blen:
            delta, pos = _read_vlq(body, pos)
            abs_tick += delta
            if pos >= blen:
                break
            b = body[pos]
            if b & 0x80:
                status = b
                pos += 1
                if b == 0xFF:                     # meta event
                    mtype = body[pos]
                    pos += 1
                    length, pos = _read_vlq(body, pos)
                    if mtype == 0x51 and length == 3:
                        tempo_ticks.append(abs_tick)
                        tempo_vals.append((body[pos] << 16) | (body[pos + 1] << 8) | body[pos + 2])
                        pos += length
                    elif mtype == 0x2F:           # end of track
                        pos += length
                        break
                    else:
                        pos += length
                    status = 0
                    continue
                if b == 0xF0 or b == 0xF7:        # sysex — skip
                    length, pos = _read_vlq(body, pos)
                    pos += length
                    status = 0
                    continue
                # else: channel status byte; its data bytes follow at pos
            else:
                if status == 0 or status >= 0xF0:
                    pos += 1                       # stray/unknown byte — skip defensively
                    continue
                # running status: b is already the first data byte (don't advance)
            hi = status & 0xF0
            chan = status & 0x0F
            if hi == 0x90:                         # note on (vel>0) or off (vel==0)
                note = body[pos]
                vel = body[pos + 1]
                pos += 2
                if vel > 0:
                    active[(chan, note)] = (abs_tick, vel)
                else:
                    onv = active.pop((chan, note), None)
                    if onv is not None:
                        st_a(onv[0]); et_a(abs_tick)
                        mi_a(note & 0x7F); tk_a(tclamp); ve_a(onv[1] if onv[1] <= 127 else 127)
                        note_total += 1
            elif hi == 0x80:                       # note off (2 data bytes: note, off-vel)
                note = body[pos]
                pos += 2
                onv = active.pop((chan, note), None)
                if onv is not None:
                    st_a(onv[0]); et_a(abs_tick)
                    mi_a(note & 0x7F); tk_a(tclamp); ve_a(onv[1] if onv[1] <= 127 else 127)
                    note_total += 1
            elif hi == 0xA0 or hi == 0xB0 or hi == 0xE0:
                pos += 2
            else:                                  # 0xC0 / 0xD0 — 1 data byte
                pos += 1
            if note_total > MAX_NOTES:
                raise ValueError("MIDI too large (over %d notes)" % MAX_NOTES)

        if active:                                 # close hanging notes at the track's last tick
            for (chan, note), (start_tick, vel) in active.items():
                st_a(start_tick); et_a(abs_tick)
                mi_a(note & 0x7F); tk_a(tclamp); ve_a(vel if vel <= 127 else 127)
                note_total += 1

    track_count = track_index + 1 if track_index >= 0 else 0
    n = note_total

    if n == 0:
        header = {
            "format": FORMAT, "noteCount": 0, "durationMs": 0,
            "trackCount": track_count, "recordBytes": REC_BYTES,
            "notesPerChunk": DEFAULT_NOTES_PER_CHUNK, "chunkCount": 0,
        }
        return header, b""

    bt, cum, tp = _build_tempo_map(tempo_ticks, tempo_vals, tpb)

    dt = np.dtype([("s", "<u4"), ("d", "<u2"), ("m", "u1"), ("t", "u1"), ("v", "u1")])
    assert dt.itemsize == REC_BYTES, "record dtype must be %d bytes, got %d" % (REC_BYTES, dt.itemsize)
    out = np.empty(n, dtype=dt)                # the packed records, built in place then sorted

    # ticks -> ms, filled straight into the record array in bounded blocks so the
    # float64 temporaries never exceed one block (keeps peak RAM low for huge files)
    STi = np.frombuffer(st, dtype=np.int32)
    ETi = np.frombuffer(et, dtype=np.int32)
    out["m"] = np.frombuffer(mi, dtype=np.uint8)
    out["t"] = np.frombuffer(tk, dtype=np.uint8)
    out["v"] = np.frombuffer(ve, dtype=np.uint8)
    max_end_s = 0.0
    for lo in range(0, n, _CONV_BLOCK):
        hi = min(lo + _CONV_BLOCK, n)
        ss = _ticks_to_seconds(STi[lo:hi].astype(np.int64), bt, cum, tp, tpb, smpte, tps)
        ee = _ticks_to_seconds(ETi[lo:hi].astype(np.int64), bt, cum, tp, tpb, smpte, tps)
        sm = np.floor(ss * 1000.0 + 0.5)
        np.clip(sm, 0, 0xFFFFFFFF, out=sm)
        out["s"][lo:hi] = sm.astype(np.uint32)
        dd = np.floor((ee - ss) * 1000.0 + 0.5)
        np.clip(dd, 1, 65535, out=dd)
        out["d"][lo:hi] = dd.astype(np.uint16)
        block_max = float(ee.max())
        if block_max > max_end_s:
            max_end_s = block_max
    del STi, ETi, st, et, mi, tk, ve          # free the parse buffers before sorting

    out.sort(order="s", kind="stable")        # in-place, stable, ascending by start_ms
    packed = out.tobytes()
    del out

    header = {
        "format": FORMAT,
        "noteCount": n,
        "durationMs": int(max_end_s * 1000 + 0.5),
        "trackCount": track_count,
        "recordBytes": REC_BYTES,
        "notesPerChunk": DEFAULT_NOTES_PER_CHUNK,
        "chunkCount": (n + DEFAULT_NOTES_PER_CHUNK - 1) // DEFAULT_NOTES_PER_CHUNK,
    }
    return header, packed


def chunk_bytes(packed: bytes, index: int, notes_per_chunk: int = DEFAULT_NOTES_PER_CHUNK) -> bytes:
    """Return the raw bytes for chunk `index` (0-based)."""
    step = notes_per_chunk * REC_BYTES
    start = index * step
    if start >= len(packed):
        return b""
    return packed[start:start + step]


def unpack(packed: bytes):
    """Debug/test helper: unpack the whole blob back into note tuples."""
    out = []
    unpack_from = REC.unpack_from
    for off in range(0, len(packed), REC_BYTES):
        out.append(unpack_from(packed, off))
    return out
