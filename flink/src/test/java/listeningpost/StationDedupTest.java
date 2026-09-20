package listeningpost;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StationDedupTest {
    static final Instant BASE = Instant.parse("2026-09-14T15:00:00Z");
    static final String A = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0";
    static final String B = "MSG,4,1,1,A22123,1,2026/09/14,16:05:24.647,2026/09/14,16:05:24.684,,,118,240,,,0,,,,,0";
    static final String C = "MSG,4,1,1,ABCDEF,1,2026/09/14,16:05:24.647,2026/09/14,16:05:24.684,,,119,240,,,0,,,,,0";

    KeyedOneInputStreamOperatorTestHarness<String, Decoded, Decoded> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(new StationDedup(), (Decoded d) -> d.stationId, Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    List<Decoded> out() {
        List<Decoded> out = harness.extractOutputValues();
        harness.getOutput().clear();
        return out;
    }

    void send(String station, Instant ts, String raw) throws Exception {
        harness.processElement(StateFunctionTest.msg(station, ts, raw), ts.toEpochMilli());
    }

    @Test
    void markIsAFlinkPojo() {
        assertInstanceOf(PojoTypeInfo.class, TypeExtractor.createTypeInfo(StationDedup.Mark.class));
    }

    @Test
    void aRetriedBatchIsDroppedWhollyAndOnlyOnce() throws Exception {
        send("s1", BASE, A);
        send("s1", BASE.plusSeconds(1), B);
        assertEquals(2, out().size());

        // the same batch again: the lost-200 retry
        send("s1", BASE, A);
        send("s1", BASE.plusSeconds(1), B);
        assertEquals(0, out().size(), "a re-sent batch is a repeat, including its last event");

        send("s1", BASE.plusSeconds(2), A);
        assertEquals(1, out().size(), "a newer event passes even if its line repeats an old one");
    }

    @Test
    void twoLinesInOneMicrosecondBothPass() throws Exception {
        send("s1", BASE, B);
        send("s1", BASE, C);
        assertEquals(2, out().size());
        send("s1", BASE, C);
        assertEquals(0, out().size(), "but the same line at the same instant is a repeat");
    }

    @Test
    void stationsAreIndependent() throws Exception {
        send("s1", BASE.plusSeconds(5), A);
        send("s2", BASE, A);
        assertEquals(2, out().size(), "another station's older event is not a repeat of this one's");
    }
}
