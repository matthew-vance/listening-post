package listeningpost;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The sweep and expiry the Go loop does by polling, driven here through timers; merge rules come from the goldens. */
class StateFunctionTest {
    static final Instant BASE = Instant.parse("2026-09-14T15:00:00Z");
    static final long EXPIRE_MS = 300_000;
    static final String POSITION = "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0";
    static final String VELOCITY = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0";

    KeyedOneInputStreamOperatorTestHarness<String, Decoded, StateOut> harness;

    static Decoded msg(String station, Instant ts, String raw) {
        return Decode.decode(new Event(station, 0, ts, raw, ts));
    }

    static KeyedOneInputStreamOperatorTestHarness<String, Decoded, StateOut> newHarness() throws Exception {
        var h = ProcessFunctionTestHarnesses.forKeyedProcessFunction(new StateFunction(EXPIRE_MS), (Decoded d) -> d.icao, Types.STRING);
        h.setProcessingTime(BASE.toEpochMilli());
        return h;
    }

    @BeforeEach
    void setUp() throws Exception {
        harness = newHarness();
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

    /** The merge rules are pinned by internal/wire/testdata/merge.json, the same scenarios the Go processor runs. */
    @TestFactory
    List<DynamicTest> mergeGolden() throws Exception {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode sc : Golden.load("merge.json")) {
            tests.add(DynamicTest.dynamicTest(sc.get("name").asText(), () -> {
                // @BeforeEach runs once per factory, not per dynamic test: each scenario needs its own aircraft state
                harness.close();
                harness = newHarness();
                int i = 0;
                for (JsonNode st : sc.get("steps")) {
                    send(msg(st.get("station").asText(), Instant.parse(st.get("ts").asText()), st.get("raw").asText()));
                    List<StateOut> out = out();
                    if (st.get("emit").isNull()) {
                        assertEquals(0, out.size(), "step " + i + " must emit nothing");
                    } else {
                        assertEquals(1, out.size(), "step " + i + " must emit once");
                        assertEquals(Golden.normalize(st.get("emit")), Golden.json(out.get(0).snapshot()), "step " + i);
                    }
                    i++;
                }
            }));
        }
        return tests;
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
