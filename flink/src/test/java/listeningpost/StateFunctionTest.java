package listeningpost;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Port of internal/processor/state_test.go, plus the timer-driven sweep the Go loop does by polling. */
class StateFunctionTest {
    static final Instant BASE = Instant.parse("2026-09-14T15:00:00Z");
    static final long EXPIRE_MS = 300_000;
    static final String IDENT = "MSG,1,1,1,A22123,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0";
    static final String POSITION = "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0";
    static final String VELOCITY = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0";

    KeyedOneInputStreamOperatorTestHarness<String, Decoded, StateOut> harness;

    static Decoded msg(String station, Instant ts, String raw) {
        return Decode.decode(new Event(station, 0, ts, raw, ts));
    }

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(new StateFunction(EXPIRE_MS), d -> d.icao, Types.STRING);
        harness.setProcessingTime(BASE.toEpochMilli());
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    List<StateOut> out() {
        List<StateOut> out = harness.extractOutputValues();
        harness.getOutput().clear();
        return out;
    }

    void send(Decoded d) throws Exception {
        harness.processElement(d, d.ts.toEpochMilli());
    }

    void advance(Duration d) throws Exception {
        harness.setProcessingTime(harness.getProcessingTime() + d.toMillis());
    }

    @Test
    void aircraftStateIsAFlinkPojo() {
        // a Kryo fallback would still work but silently break state schema evolution
        assertInstanceOf(PojoTypeInfo.class, TypeExtractor.createTypeInfo(Aircraft.class));
        assertInstanceOf(PojoTypeInfo.class, TypeExtractor.createTypeInfo(Decoded.class));
    }

    @Test
    void mergesMessageTypes() throws Exception {
        send(msg("s1", BASE, POSITION));
        send(msg("s1", BASE.plusSeconds(1), VELOCITY));
        send(msg("s1", BASE.plusSeconds(2), IDENT));
        List<StateOut> out = out();
        assertEquals(3, out.size(), "every message changed something");
        Snapshot s = out.get(2).snapshot();
        assertEquals(8275, s.altitude);
        assertEquals(117, s.groundSpeed);
        assertEquals(40.14684, s.lat);
        assertEquals("AAL433", s.callsign);
        assertEquals(3, s.messages);
        assertEquals(BASE, s.firstSeen);
        assertEquals(BASE.plusSeconds(2), s.lastSeen);
        assertEquals(BASE, s.positionTs);
    }

    @Test
    void repeatWithoutChangeIsQuietUntilSweep() throws Exception {
        send(msg("s1", BASE, VELOCITY));
        send(msg("s1", BASE.plusSeconds(1), VELOCITY));
        assertEquals(1, out().size(), "identical values must not emit");

        advance(Duration.ofMillis(StateFunction.SWEEP_MS));
        List<StateOut> swept = out();
        assertEquals(1, swept.size(), "sweep republishes the aircraft heard but unchanged");
        assertEquals(2, swept.get(0).snapshot().messages);
        assertEquals(BASE.plusSeconds(1), swept.get(0).snapshot().lastSeen);

        advance(Duration.ofMillis(StateFunction.SWEEP_MS));
        assertEquals(0, out().size(), "nothing new: nothing republished");

        // a repeated position is not quiet: position_ts is the staleness signal, so re-confirming it counts
        send(msg("s1", BASE.plusSeconds(2), POSITION));
        send(msg("s1", BASE.plusSeconds(3), POSITION));
        List<StateOut> positions = out();
        assertEquals(2, positions.size());
        assertEquals(BASE.plusSeconds(3), positions.get(1).snapshot().positionTs);
    }

    @Test
    void olderMessageCannotRegressButCanFill() throws Exception {
        send(msg("s1", BASE.plusSeconds(3600), POSITION)); // live position at 8275
        String stale = "MSG,3,1,1,A22123,1,2026/09/14,14:00:00.000,2026/09/14,14:00:00.000,,2000,,,41.0,-84.0,,,0,,0,0";
        send(msg("s2", BASE, stale));
        List<StateOut> out = out();
        assertEquals(1, out.size(), "stale backlog must not emit");
        assertEquals(8275, out.get(0).snapshot().altitude);

        // but an older message still fills fields nothing newer has set
        send(msg("s2", BASE, VELOCITY));
        out = out();
        assertEquals(1, out.size());
        assertEquals(117, out.get(0).snapshot().groundSpeed);
        assertArrayEquals(new String[]{"s1", "s2"}, out.get(0).snapshot().stations);
    }

    @Test
    void expiresAfterSilence() throws Exception {
        send(msg("s1", BASE, POSITION));
        out();
        advance(Duration.ofMillis(EXPIRE_MS - StateFunction.SWEEP_MS));
        assertEquals(0, out().size(), "not yet silent long enough");

        advance(Duration.ofMillis(2 * StateFunction.SWEEP_MS));
        List<StateOut> out = out();
        assertEquals(1, out.size());
        assertEquals("A22123", out.get(0).icao());
        assertNull(out.get(0).snapshot(), "expiry is a tombstone");

        advance(Duration.ofMillis(10 * StateFunction.SWEEP_MS));
        assertEquals(0, out().size(), "timer chain ends with the aircraft");

        // heard again: starts over as a new aircraft
        send(msg("s1", BASE.plusSeconds(1000), POSITION));
        assertEquals(1, out().get(0).snapshot().messages);
    }
}
