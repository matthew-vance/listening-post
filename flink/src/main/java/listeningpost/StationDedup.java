package listeningpost;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Drops re-deliveries of a station's events. events.raw is at-least-once: the gateway publishes before it
 * answers, so a station whose 200 got lost re-sends the batch. A station's events arrive in the order it heard
 * them with monotonic timestamps, so anything at or before the newest one seen is a repeat — "at" needs the
 * message's own identity too, since one microsecond can hold two different lines.
 */
public class StationDedup extends KeyedProcessFunction<String, Decoded, Decoded> {
    private transient ValueState<Mark> mark;

    /** Flink POJO: the newest event seen from this station. */
    public static class Mark {
        public Instant ts;
        public String fingerprint;
    }

    static String fingerprint(Decoded d) {
        return d.generated + "|" + d.logged + "|" + d.icao;
    }

    @Override
    public void open(OpenContext ctx) {
        mark = getRuntimeContext().getState(new ValueStateDescriptor<>("mark", Mark.class));
    }

    @Override
    public void processElement(Decoded d, Context ctx, Collector<Decoded> out) throws Exception {
        String fp = fingerprint(d);
        Mark prev = mark.value();
        // a mark restored from a checkpoint written by an older Mark shape has null fields: treat it as no mark
        if (prev != null && prev.ts != null) {
            int order = d.ts.compareTo(prev.ts);
            if (order < 0 || (order == 0 && fp.equals(prev.fingerprint))) {
                return;
            }
        }
        Mark m = new Mark();
        m.ts = d.ts;
        m.fingerprint = fp;
        mark.update(m);
        out.collect(d);
    }
}
