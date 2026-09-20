package listeningpost;

import java.time.Instant;

/**
 * The aircraft.state_history wire format: a changed Snapshot stamped with the station and time of the raw Event
 * that triggered it. The key is deterministic (derived from the archive, not from fold order or randomness), so
 * re-folding the same events yields the same key and the writer's ON CONFLICT dedupes the re-run.
 */
public class Trace extends Snapshot {
    public String eventStationId;
    public Instant eventTs;

    /** Copies a snapshot's fields onto a fresh trace and stamps it with its triggering Event's identity. */
    public static Trace of(Snapshot s, String stationId, Instant eventTs) {
        Trace t = Json.MAPPER.convertValue(s, Trace.class);
        t.eventStationId = stationId;
        t.eventTs = eventTs;
        return t;
    }
}
