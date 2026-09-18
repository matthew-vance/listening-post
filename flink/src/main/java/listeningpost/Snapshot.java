package listeningpost;

import java.time.Instant;

/** The aircraft.state wire format: the full current picture of one aircraft, never a delta. */
public class Snapshot extends Payload {
    public String icao;
    public Instant firstSeen;
    public Instant lastSeen;
    public Instant positionTs;
    public String[] stations = new String[0];
    public long messages;
}
