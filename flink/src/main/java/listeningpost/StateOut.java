package listeningpost;

/** One record for aircraft.state: a snapshot, or a tombstone when snapshot is null. */
public record StateOut(String icao, Snapshot snapshot) {}
