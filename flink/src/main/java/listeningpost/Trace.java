package listeningpost;

import java.time.Instant;

/**
 * The aircraft.state_history wire format: a changed Snapshot stamped with the identity of the raw Event that
 * triggered it. The key is deterministic (derived from the archive, not from fold order or randomness), so
 * re-folding the same events yields the same key and the writer's ON CONFLICT dedupes the re-run.
 */
public class Trace extends Snapshot {
    public String eventStationId;
    public long eventId;
    public Instant eventTs;

    /** Copies a snapshot's fields onto a fresh trace and stamps it with its triggering Event's identity. Explicit
     *  field copy rather than a mapper round-trip, so a renamed field breaks here at compile time. */
    public static Trace of(Snapshot s, String stationId, long eventId, Instant eventTs) {
        Trace t = new Trace();
        t.icao = s.icao;
        t.callsign = s.callsign;
        t.altitude = s.altitude;
        t.groundSpeed = s.groundSpeed;
        t.track = s.track;
        t.lat = s.lat;
        t.lon = s.lon;
        t.verticalRate = s.verticalRate;
        t.squawk = s.squawk;
        t.alert = s.alert;
        t.emergency = s.emergency;
        t.spi = s.spi;
        t.onGround = s.onGround;
        t.firstSeen = s.firstSeen;
        t.lastSeen = s.lastSeen;
        t.positionTs = s.positionTs;
        t.stations = s.stations;
        t.messages = s.messages;
        t.eventStationId = stationId;
        t.eventId = eventId;
        t.eventTs = eventTs;
        return t;
    }
}
