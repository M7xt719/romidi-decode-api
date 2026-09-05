"""
Ro-MIDI decode core — parse a .mid into a compact, chunkable note schedule.

Wire format  (format id: "romidi-sched-v1")
-------------------------------------------
The schedule is a flat array of fixed 9-byte note records, little-endian:

    <  I  H  B  B  B  >
       |  |  |  |  |
       |  |  |  |  +-- velocity   (u8,  0..127)
       |  |  |  +----- track      (u8,  0..255, clamped)
       |  |  +-------- midi note  (u8,  0..127)
       |  +----------- duration   (u16, milliseconds, 1..65535 clamped)
       +-------------- start      (u32, milliseconds from song start)

Notes are sorted by start time. The array is served in chunks of
`notesPerChunk` records, so every HTTP response stays well under Roblox's
~1 MB transfer limit. Roblox reads each chunk with buffer.fromstring +
buffer.readu32/readu16/readu8 (no JSON, no base64).
"""

import io
import struct
import bisect
import mido

REC = struct.Struct("<IHBBB")   # 9 bytes / note
REC_BYTES = REC.size            # 9
FORMAT = "romidi-sched-v1"

# safety caps so a hostile / insane file can't OOM the server
MAX_NOTES = 5_000_000
DEFAULT_NOTES_PER_CHUNK = 60_000   # 60000 * 9 = 540 KB per chunk (safe under ~1 MB)


def _tempo_map(mid):
    """Return (boundary_ticks, tempos, cum_seconds, ticks_per_beat).

    boundary_ticks[i] is an absolute tick where the tempo becomes tempos[i];
    cum_seconds[i] is the elapsed seconds at that boundary. This lets us convert
    any tick to seconds in O(log n) with a bisect, correct across tempo changes.
    """
    raw = []
    for track in mid.tracks:
        t = 0
        for msg in track:
            t += msg.time
            if msg.type == "set_tempo":
                raw.append((t, msg.tempo))
    raw.sort(key=lambda x: x[0])

    boundary_ticks, tempos = [], []
    for tick, tempo in raw:
        if boundary_ticks and boundary_ticks[-1] == tick:
            tempos[-1] = tempo            # last tempo set at this tick wins
        else:
            boundary_ticks.append(tick)
            tempos.append(tempo)
    if not boundary_ticks or boundary_ticks[0] != 0:
        boundary_ticks.insert(0, 0)
        tempos.insert(0, 500000)          # default 120 BPM

    tpb = mid.ticks_per_beat or 480
    cum = [0.0]
    for i in range(1, len(boundary_ticks)):
        dt_ticks = boundary_ticks[i] - boundary_ticks[i - 1]
        cum.append(cum[-1] + dt_ticks / tpb * (tempos[i - 1] / 1_000_000.0))
    return boundary_ticks, tempos, cum, tpb


def parse_midi(data: bytes):
    """Parse raw .mid bytes -> (header_dict, packed_bytes)."""
    mid = mido.MidiFile(file=io.BytesIO(data))
    boundary_ticks, tempos, cum, tpb = _tempo_map(mid)

    def t2s(tick):
        i = bisect.bisect_right(boundary_ticks, tick) - 1
        return cum[i] + (tick - boundary_ticks[i]) / tpb * (tempos[i] / 1_000_000.0)

    notes = []   # (start_s, end_s, midi, track, vel)
    for ti, track in enumerate(mid.tracks):
        abs_tick = 0
        active = {}   # (channel, note) -> (start_tick, vel)
        for msg in track:
            abs_tick += msg.time
            mtype = msg.type
            if mtype == "note_on" and msg.velocity > 0:
                active[(msg.channel, msg.note)] = (abs_tick, msg.velocity)
            elif mtype == "note_off" or (mtype == "note_on" and msg.velocity == 0):
                st = active.pop((msg.channel, msg.note), None)
                if st is not None:
                    start_tick, vel = st
                    notes.append((t2s(start_tick), t2s(abs_tick), msg.note, ti, vel))
                    if len(notes) > MAX_NOTES:
                        raise ValueError("MIDI too large (over %d notes)" % MAX_NOTES)
        for (ch, note), (start_tick, vel) in active.items():   # close hanging notes
            notes.append((t2s(start_tick), t2s(abs_tick), note, ti, vel))

    notes.sort(key=lambda n: n[0])

    buf = bytearray(len(notes) * REC_BYTES)
    off = 0
    max_end = 0.0
    pack_into = REC.pack_into
    for (s, e, note, ti, vel) in notes:
        start_ms = int(s * 1000 + 0.5)
        dur_ms = int((e - s) * 1000 + 0.5)
        if dur_ms < 1:
            dur_ms = 1
        elif dur_ms > 65535:
            dur_ms = 65535
        if start_ms < 0:
            start_ms = 0
        elif start_ms > 0xFFFFFFFF:
            start_ms = 0xFFFFFFFF
        pack_into(buf, off, start_ms, dur_ms, note & 0x7F,
                  ti if ti < 255 else 255, vel if vel <= 127 else 127)
        off += REC_BYTES
        if e > max_end:
            max_end = e

    header = {
        "format": FORMAT,
        "noteCount": len(notes),
        "durationMs": int(max_end * 1000 + 0.5),
        "trackCount": len(mid.tracks),
        "recordBytes": REC_BYTES,
        "notesPerChunk": DEFAULT_NOTES_PER_CHUNK,
        "chunkCount": (len(notes) + DEFAULT_NOTES_PER_CHUNK - 1) // DEFAULT_NOTES_PER_CHUNK,
    }
    return header, bytes(buf)


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
    for off in range(0, len(packed), REC_BYTES):
        out.append(REC.unpack_from(packed, off))
    return out
