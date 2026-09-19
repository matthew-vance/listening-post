package listeningpost;

/** The aircraft.state_history wire format: a changed Snapshot stamped with the processor-minted idempotency key. */
public class Trace extends Snapshot {
    public String id;

    /** Copies a snapshot's fields onto a fresh trace and stamps it with the id the writer dedupes on. */
    public static Trace of(Snapshot snap, String id) {
        Trace t = Json.MAPPER.convertValue(snap, Trace.class);
        t.id = id;
        return t;
    }
}
