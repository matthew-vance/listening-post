package listeningpost;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Sweep and expiry, driven through timers; merge rules come from the goldens. */
class StateFunctionTest {
    static final Instant BASE = Instant.parse("2026-09-14T15:00:00Z");
    static final long EXPIRE_MS = 300_000;
    static final String POSITION = "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0";
    static final String VELOCITY = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0";

    KeyedOneInputStreamOperatorTestHarness<String, Decoded, StateOut> harness;

    static Decoded msg(String station, Instant ts, String raw) {
        return Decode.decode(new Event(station, 0, ts, raw, ts));
    }

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(new StateFunction(EXPIRE_MS), (Decoded d) -> d.icao, Types.STRING);
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

    List<Trace> traces() {
        var q = harness.getSideOutput(StateFunction.TRACES);
        List<Trace> out = new ArrayList<>();
        q.forEach(r -> out.add(r.getValue()));
        q.clear();
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
        assertInstanceOf(PojoTypeInfo.class, TypeExtractor.createTypeInfo(Trace.class));
    }

    @Test
    void sweepRepublishesHeardButUnchanged() throws Exception {
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
    }

    @Test
    void tracesFollowChangesNotSweeps() throws Exception {
        send(msg("s1", BASE, VELOCITY));
        assertEquals(1, out().size());
        List<Trace> changed = traces();
        assertEquals(1, changed.size(), "a change lands one trace");
        assertEquals("A22123", changed.get(0).icao);

        send(msg("s1", BASE.plusSeconds(1), VELOCITY)); // identical: no change
        assertEquals(0, out().size());
        assertEquals(0, traces().size(), "no change, no trace");

        advance(Duration.ofMillis(StateFunction.SWEEP_MS)); // heard-but-unchanged: state only
        assertEquals(1, out().size());
        assertEquals(0, traces().size(), "the sweep republish is not a trace");

        advance(Duration.ofMillis(EXPIRE_MS)); // silent long enough: tombstone only
        assertEquals(1, out().size());
        assertEquals(0, traces().size(), "a tombstone is not a trace");
    }

    /** The trace wire shape is pinned by internal/wire/testdata/trace.json. */
    @Test
    void traceSerializationGolden() throws Exception {
        Snapshot s = new Snapshot();
        s.icao = "A22123";
        s.firstSeen = Instant.parse("2026-09-14T15:00:00Z");
        s.lastSeen = Instant.parse("2026-09-14T15:00:02Z");
        s.positionTs = Instant.parse("2026-09-14T15:00:02Z");
        s.stations = new String[]{"s1", "s2"};
        s.messages = 3;
        s.callsign = "AAL433";
        s.altitude = 8275;
        s.groundSpeed = 117.0;
        s.track = 240.0;
        s.lat = 40.14684;
        s.lon = -83.17065;
        s.verticalRate = 0;
        s.squawk = "6653";
        s.alert = false;
        s.emergency = false;
        s.spi = false;
        s.onGround = false;

        Trace t = Trace.of(s, "9f8b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d");
        assertEquals(Golden.normalize(Golden.load("trace.json")), Golden.normalize(t));
    }

    static List<Named<JsonNode>> mergeScenarios() throws Exception {
        List<Named<JsonNode>> scenarios = new ArrayList<>();
        for (JsonNode sc : Golden.load("merge.json")) {
            scenarios.add(Named.of(sc.get("name").asText(), sc));
        }
        return scenarios;
    }

    /** The merge rules are pinned by internal/wire/testdata/merge.json. */
    @ParameterizedTest
    @MethodSource("mergeScenarios")
    void mergeGolden(JsonNode sc) throws Exception {
        int i = 0;
        for (JsonNode st : sc.get("steps")) {
            send(msg(st.get("station").asText(), Instant.parse(st.get("ts").asText()), st.get("raw").asText()));
            List<StateOut> out = out();
            if (st.get("emit").isNull()) {
                assertEquals(0, out.size(), "step " + i + " must emit nothing");
            } else {
                assertEquals(1, out.size(), "step " + i + " must emit once");
                assertEquals(Golden.normalize(st.get("emit")), Golden.normalize(out.get(0).snapshot()), "step " + i);
            }
            i++;
        }
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
