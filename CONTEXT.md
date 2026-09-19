# Listening Post

An ADS-B receiving pipeline: stations on Raspberry Pis forward what dump1090 hears to a server that decodes it, folds it into a live picture of the sky, and archives it.

## Language

**Station**:
One receiver installation, identified by a UUID and authenticated by bearer token. Everything it hears is attributed to it.

**Buffer**:
The SQLite file on a station that holds Events from the moment dump1090 emits them until the gateway has acknowledged them. It is what lets a station survive reboots and gateway outages without losing data.
_Avoid_: db, store, queue, events table

**Event**:
One raw SBS-1 line as a station heard it, wrapped with the station id, the station's sequence number, and timestamps. The unit of everything upstream of decoding.
_Avoid_: message, line, record

**Decoded**:
An Event with its SBS-1 line parsed into typed fields, keyed by the aircraft it concerns. Derived, never edited: the raw Event is the source of truth.
_Avoid_: parsed message, decoded record

**Payload**:
The twelve SBS-1 attributes that travel with an aircraft (callsign, altitude, speed, track, position, vertical rate, squawk, flags). Present on a Decoded when the line carried them, and on a Snapshot as the latest known values.
_Avoid_: fields, attributes, SBS block

**Snapshot**:
The full current picture of one aircraft: its latest Payload plus when it was first and last heard, which stations hear it, and how many messages contributed. Always whole, never a delta; replaced, never patched.
_Avoid_: state, aircraft state, update, delta

**Tombstone**:
The record that retires an aircraft's Snapshot once it has been silent for the expiry window.
_Avoid_: delete, null record

**Trace**:
One aircraft's changed Snapshots recorded over time, append-only, for drawing a flight path later. A new Trace lands only when the Snapshot's Payload actually changes, so it carries the aircraft's history without the sweep republishes. Each Trace is keyed by the raw Event that triggered it (station, its sequence number, and its time), so re-folding the same Events yields the same Traces.
_Avoid_: track (already the SBS heading field)
