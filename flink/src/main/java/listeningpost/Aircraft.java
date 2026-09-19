package listeningpost;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Keyed state for one aircraft: its snapshot plus the event time at which each field was last set. A port of
 * internal/processor/state.go. Flink POJO: public fields, no-arg constructor, only basic and array field types.
 */
public class Aircraft {
    // indexes into fieldTs; a long[] rather than a Map so Flink's POJO serializer handles it without Kryo
    static final int CALLSIGN = 0, ALTITUDE = 1, GROUND_SPEED = 2, TRACK = 3, LAT = 4, LON = 5, VERTICAL_RATE = 6,
            SQUAWK = 7, ALERT = 8, EMERGENCY = 9, SPI = 10, ON_GROUND = 11, POSITION_TS = 12;

    public Snapshot snap = new Snapshot();
    public long[] fieldTs = new long[13]; // epoch millis; 0 = never set
    public boolean dirty; // heard from since its last snapshot went out

    public static Aircraft create(String icao) {
        Aircraft a = new Aircraft();
        a.snap.icao = icao;
        return a;
    }

    /**
     * Merges one decoded message and reports whether any field's value changed. A field updates only if the
     * message is at least as new as the one that last set it: a station's stale backlog can't regress live state,
     * but a slightly reordered message from another station still lands the fields the newer one lacked.
     */
    public boolean apply(Decoded m) {
        Snapshot s = snap;
        if (s.firstSeen == null || m.ts.isBefore(s.firstSeen)) {
            s.firstSeen = m.ts;
        }
        if (s.lastSeen == null || m.ts.isAfter(s.lastSeen)) {
            s.lastSeen = m.ts;
        }
        s.messages++;
        if (Arrays.binarySearch(s.stations, m.stationId) < 0) {
            String[] stations = Arrays.copyOf(s.stations, s.stations.length + 1);
            stations[s.stations.length] = m.stationId;
            Arrays.sort(stations);
            s.stations = stations;
        }

        long ts = m.ts.toEpochMilli();
        boolean changed = false;
        changed |= set(CALLSIGN, ts, m.callsign, s.callsign, v -> s.callsign = v);
        changed |= set(ALTITUDE, ts, m.altitude, s.altitude, v -> s.altitude = v);
        changed |= set(GROUND_SPEED, ts, m.groundSpeed, s.groundSpeed, v -> s.groundSpeed = v);
        changed |= set(TRACK, ts, m.track, s.track, v -> s.track = v);
        changed |= set(LAT, ts, m.lat, s.lat, v -> s.lat = v);
        changed |= set(LON, ts, m.lon, s.lon, v -> s.lon = v);
        changed |= set(VERTICAL_RATE, ts, m.verticalRate, s.verticalRate, v -> s.verticalRate = v);
        changed |= set(SQUAWK, ts, m.squawk, s.squawk, v -> s.squawk = v);
        changed |= set(ALERT, ts, m.alert, s.alert, v -> s.alert = v);
        changed |= set(EMERGENCY, ts, m.emergency, s.emergency, v -> s.emergency = v);
        changed |= set(SPI, ts, m.spi, s.spi, v -> s.spi = v);
        changed |= set(ON_GROUND, ts, m.onGround, s.onGround, v -> s.onGround = v);
        boolean hasPosition = m.lat != null && m.lon != null;
        changed |= set(POSITION_TS, ts, hasPosition ? m.ts : null, s.positionTs, v -> s.positionTs = v);
        dirty = !changed; // heard: a changed snapshot goes out now, an unchanged one is owed on the next sweep
        return changed;
    }

    /** Applies v (null = absent from the message) to a field if the message is at least as new as its last setter. */
    private <T> boolean set(int field, long ts, T v, T cur, Consumer<T> assign) {
        if (v == null || ts < fieldTs[field]) {
            return false;
        }
        fieldTs[field] = ts;
        if (Objects.equals(cur, v)) {
            return false;
        }
        assign.accept(v);
        return true;
    }

    /** Silent for longer than expireMs as of now (processing time against event time, same as the Go sweep). */
    public boolean expired(long nowMs, long expireMs) {
        return nowMs - snap.lastSeen.toEpochMilli() > expireMs;
    }
}
