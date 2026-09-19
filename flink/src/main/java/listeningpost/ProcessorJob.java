package listeningpost;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The processor: decodes events.raw onto events.decoded and folds that into aircraft.state. Decode is a stateless
 * flatMap; state is a keyed process function whose state Flink checkpoints. Topic names are pinned: auto-create
 * is off and compose's kafka-init declares exactly these.
 */
public final class ProcessorJob {
    static final String RAW = "events.raw";
    static final String DECODED = "events.decoded";     // parsed lines, keyed by ICAO
    static final String STATE = "aircraft.state";        // compacted: latest snapshot per aircraft, keyed by ICAO
    static final String STATE_HISTORY = "aircraft.state_history"; // append-only: changed snapshots, keyed by ICAO

    public static void main(String[] args) throws Exception {
        String brokers = Objects.requireNonNull(System.getenv("KAFKA_BROKERS"), "KAFKA_BROKERS is not set");
        long expireMs = Long.parseLong(Objects.requireNonNullElse(System.getenv("EXPIRE_SECONDS"), "300")) * 1000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(RAW)
                .setGroupId("flink-processor")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Decoded> decoded = env.fromSource(source, WatermarkStrategy.noWatermarks(), RAW)
                .uid("raw-source")
                .flatMap(new Decode()).name("decode").uid("decode");
        decoded.sinkTo(sink(brokers, DECODED, Decoded::key, d -> d)).name(DECODED).uid("decoded-sink");
        var state = decoded.filter(d -> d.icao != null) // nothing to key state on
                .uid("icao-filter")
                .keyBy(d -> d.icao)
                .process(new StateFunction(expireMs)).name("state").uid("state");
        state.sinkTo(sink(brokers, STATE, StateOut::icao, StateOut::snapshot)).name(STATE).uid("state-sink");
        state.getSideOutput(StateFunction.TRACES)
                .sinkTo(sink(brokers, STATE_HISTORY, t -> t.icao, t -> t)).name(STATE_HISTORY).uid("state-history-sink");
        env.execute("processor");
    }

    /**
     * A JSON Kafka sink; a null value from the value function becomes a tombstone. Exactly-once: records are
     * written in a Kafka transaction that commits with the checkpoint, so a crash rolls output back together with
     * state. Consumers must read_committed. The transaction timeout must not exceed the broker's
     * transaction.max.timeout.ms (15 min by default); Flink's own default of one hour would.
     */
    static <T> KafkaSink<T> sink(String brokers, String topic, SerializableFunction<T, String> key, SerializableFunction<T, Object> value) {
        KafkaRecordSerializationSchema<T> schema = (element, context, timestamp) -> {
            try {
                Object v = value.apply(element);
                byte[] bytes = v == null ? null : Json.MAPPER.writeValueAsBytes(v);
                return new ProducerRecord<>(topic, key.apply(element).getBytes(StandardCharsets.UTF_8), bytes);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        };
        return KafkaSink.<T>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(schema)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(topic)
                .setProperty("transaction.timeout.ms", "600000")
                .build();
    }

    private ProcessorJob() {}
}
