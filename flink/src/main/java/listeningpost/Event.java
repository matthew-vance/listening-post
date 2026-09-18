package listeningpost;

import java.time.Instant;

/** The events.raw envelope: one SBS-1 line from one station (see internal/wire/wire.go). */
public record Event(String stationId, long id, Instant ts, String raw, Instant receivedAt) {}
