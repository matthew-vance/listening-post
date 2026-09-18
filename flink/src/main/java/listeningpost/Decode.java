package listeningpost;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes one events.raw record. Lines that don't parse are logged and dropped: the raw archive keeps them. */
public class Decode implements FlatMapFunction<String, Decoded> {
    private static final Logger LOG = LoggerFactory.getLogger(Decode.class);

    static Decoded decode(String value) throws Exception {
        return decode(Json.MAPPER.readValue(value, Event.class));
    }

    static Decoded decode(Event e) {
        Decoded d = Sbs.parse(e.raw());
        d.stationId = e.stationId();
        d.id = e.id();
        d.ts = e.ts();
        d.receivedAt = e.receivedAt();
        return d;
    }

    @Override
    public void flatMap(String value, Collector<Decoded> out) {
        try {
            out.collect(decode(value));
        } catch (Exception e) {
            LOG.warn("skipping record: {}", e.getMessage());
        }
    }
}
