package listeningpost;

import java.time.Instant;

/** The events.decoded wire format: the envelope minus the raw line, plus the parsed message. */
public class Decoded extends Payload {
    public String stationId;
    public Instant ts;
    public Instant receivedAt;
    public String messageType;
    public Integer transmissionType;
    public String icao;
    // Receiver clock in its local zone with no offset: kept verbatim. The envelope's ts is the event time.
    public String generated;
    public String logged;

    /** Kafka key for events.decoded: the aircraft, or the station when the line names none. */
    public String key() {
        return icao != null ? icao : stationId;
    }
}
