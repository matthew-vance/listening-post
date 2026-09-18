package listeningpost;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Folds decoded messages into per-aircraft state and publishes a snapshot per change. A processing-time timer
 * per aircraft fires every SWEEP_MS: it tombstones aircraft silent longer than expireMs and republishes ones
 * heard from since their last snapshot but unchanged, so last_seen and messages don't go stale on the topic.
 * Processing time rather than event time because an idle receiver would stall watermarks and nothing would
 * ever expire.
 */
public class StateFunction extends KeyedProcessFunction<String, Decoded, StateOut> {
    static final long SWEEP_MS = 10_000;

    private final long expireMs;
    private transient ValueState<Aircraft> state;

    public StateFunction(long expireMs) {
        this.expireMs = expireMs;
    }

    @Override
    public void open(OpenContext ctx) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("aircraft", Aircraft.class));
    }

    @Override
    public void processElement(Decoded m, Context ctx, Collector<StateOut> out) throws Exception {
        Aircraft a = state.value();
        if (a == null) {
            a = Aircraft.create(m.icao);
            // one timer chain per live aircraft; onTimer keeps it going until the aircraft expires
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + SWEEP_MS);
        }
        if (a.apply(m)) {
            a.dirty = false; // this snapshot is going out now
            out.collect(new StateOut(m.icao, a.snap));
        }
        state.update(a);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<StateOut> out) throws Exception {
        Aircraft a = state.value();
        if (a == null) {
            return;
        }
        if (a.expired(ctx.timerService().currentProcessingTime(), expireMs)) {
            out.collect(new StateOut(ctx.getCurrentKey(), null));
            state.clear();
            return;
        }
        if (a.dirty) {
            a.dirty = false;
            out.collect(new StateOut(ctx.getCurrentKey(), a.snap));
            state.update(a);
        }
        ctx.timerService().registerProcessingTimeTimer(timestamp + SWEEP_MS);
    }
}
