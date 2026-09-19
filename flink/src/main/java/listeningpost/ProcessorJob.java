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
 * The Go processor (internal/processor) re-implemented as one Flink job, reading the same events.raw but writing
 * to its own topics so the two can be compared. Decode is a stateless flatMap; state is a keyed process function,
 * so Flink's checkpoints replace the Go side's warm-up-from-compacted-topic and partition alignment.
 */
public final class ProcessorJob {
    static final String RAW = "events.raw";
    static final String DECODED = "events.decoded.flink";
    static final String STATE = "aircraft.state.flink";

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
                .flatMap(new Decode()).name("decode");
        decoded.sinkTo(sink(brokers, DECODED, Decoded::key, d -> d)).name(DECODED);
        decoded.filter(d -> d.icao != null) // nothing to key state on
                .keyBy(d -> d.icao)
                .process(new StateFunction(expireMs)).name("state")
                .sinkTo(sink(brokers, STATE, StateOut::icao, StateOut::snapshot)).name(STATE);
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
