package loghub.receivers;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import loghub.BuildableConnectionContext;
import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.decoders.Decoder;
import loghub.events.Event;
import loghub.kafka.KafkaProperties;
import loghub.kafka.KeyTypes;
import loghub.kafka.range.RangeCollection;
import loghub.security.ssl.ClientAuthentication;
import loghub.types.MimeType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Blocking
@BuilderClass(Kafka.Builder.class)
public class Kafka extends Receiver<Kafka, Kafka.Builder> {

    public static class KafkaContext extends BuildableConnectionContext<Object> implements Cloneable {
        @Getter
        private final String topic;
        @Getter
        private final int partition;
        KafkaContext(String topic, int partition, Runnable onAcknowledge) {
            this.topic = topic;
            this.partition = partition;
            setOnAcknowledge(onAcknowledge);
        }
        @Override
        public Object getLocalAddress() {
            return null;
        }
        @Override
        public Object getRemoteAddress() {
            return null;
        }
        public Object clone() {
            KafkaContext kc = new KafkaContext(topic, partition, () -> {});
            kc.setPrincipal(getPrincipal());
            return kc;
        }
    }

    @Setter @Getter
    public static class Builder extends Receiver.Builder<Kafka, Kafka.Builder> implements KafkaProperties {
        private String[] brokers = new String[] { "localhost"};
        private int port = 9092;
        private String topic = "LogHub";
        private String group = "LogHub";
        private Class<?> keyClass;
        private ClassLoader classLoader = Kafka.class.getClassLoader();
        private String compressionType;
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuthentication;
        private boolean withAutoCommit = true;
        private Map<String, Object> kafkaProperties = Map.of();
        private Map<String, Decoder> decoders = Map.of();
        // Only used for tests
        @Setter(AccessLevel.PACKAGE)
        private Consumer<byte[], byte[]> consumer;
        @Override
        public Kafka build() {
            return new Kafka(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final String topic;
    private Supplier<Consumer<byte[], byte[]>> consumerSupplier;
    private final Map<Integer, RangeCollection> ranges = new ConcurrentHashMap<>();
    private final Class<?> keyClass;
    private final boolean withAutoCommit;
    private final Map<MimeType, Decoder> decoders;

    protected Kafka(Builder builder) {
        super(builder);
        this.topic = builder.topic;
        if (builder.consumer != null) {
            consumerSupplier = () -> builder.consumer;
        } else {
            consumerSupplier = getConsumer(builder);
        }
        if (builder.keyClass != null) {
            keyClass = builder.keyClass;
        } else {
            keyClass = null;
        }
        withAutoCommit = builder.withAutoCommit;
        decoders = resolverDecoders(builder.decoders);
    }

    private Supplier<Consumer<byte[], byte[]>> getConsumer(Builder builder) {
        Map<String, Object> props = builder.configureKafka(logger);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, withAutoCommit);
        return () -> {
            try {
                Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                consumer.subscribe(List.of(topic));
                return consumer;
            } catch (KafkaException ex) {
                logger.atError().withThrowable(ex).log("Failed to configure Kafka consumer: {}", Helpers.resolveThrowableException(ex));
                throw ex;
            }
        };
    }

    @Override
    public String getReceiverName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

    public Consumer<byte[], byte[]> getConsumer() {
        try {
            return consumerSupplier.get();
        } finally {
            consumerSupplier = null;
        }
    }

    @Override
    public void run() {
        try (Consumer<byte[], byte[]> consumer = getConsumer()) {
            Duration pollingInterval = Duration.ofMillis(100);
            while (! isInterrupted()) {
                if (! eventLoop(consumer, pollingInterval)) {
                    break;
                }
            }
            commit(consumer);
            close();
        } catch (KafkaException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Kafka receiver failed: {}", () -> Helpers.resolveThrowableException(ex));
        }
    }

    private boolean eventLoop(Consumer<byte[], byte[]> consumer, Duration pollingInterval) {
        try {
            ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(pollingInterval);
            if (consumerRecords.count() != 0) {
                logger.debug("Received {} records", consumerRecords::count);
                processRecords(consumer, consumerRecords);
            }
            commit(consumer);
            return true;
        } catch (WakeupException | InterruptException e) {
            return false;
        } catch (KafkaException ex) {
            logger.atError().withThrowable(ex).log("Failed Kafka received: {}", Helpers.resolveThrowableException(ex));
            return true;
        }
    }

    private void commit(Consumer<byte[], byte[]> consumer) {
        if (withAutoCommit) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (Map.Entry<Integer, RangeCollection> i: ranges.entrySet()) {
            RangeCollection range = i.getValue();
            long lastAck = range.merge();
            if (lastAck >= 0) {
                TopicPartition tp = new TopicPartition(this.topic, i.getKey());
                // commit is not the last accepted offset, but the next expected offset
                toCommit.put(tp, new OffsetAndMetadata(lastAck + 1));
            }
        }
        if (! toCommit.isEmpty()) {
            consumer.commitAsync(toCommit, this::onComplete);
        }
    }

    private void onComplete(Map<TopicPartition,OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? exception : null)
                  .log("Failed commit for {}: {}", () -> offsets, () -> Helpers.resolveThrowableException(exception));
        }
    }

    void processRecords(Consumer<byte[], byte[]> consumer, ConsumerRecords<byte[], byte[]> consumerRecords) {
        for (ConsumerRecord<byte[], byte[]> kafkaRecord: consumerRecords) {
            logger.trace("Got a record {}", kafkaRecord);
            KafkaContext ctxt = new KafkaContext(
                    kafkaRecord.topic(),
                    kafkaRecord.partition(),
                    () -> getPartitionRange(kafkaRecord.partition()).addValue(kafkaRecord.offset())
            );
            Optional.ofNullable(kafkaRecord.headers().lastHeader("Content-Type"))
                    .map(h -> new String(h.value(), StandardCharsets.UTF_8))
                    .map(decoders::get)
                    .ifPresent(ctxt::setDecoder);
            kafkaRecord.headers().remove("Content-Type");
            Optional<Date> timestamp = Optional.ofNullable(kafkaRecord.timestampType() == TimestampType.CREATE_TIME ? new Date(kafkaRecord.timestamp()) : null);
            byte[] content = kafkaRecord.value();
            decodeStream(ctxt, content).forEach(e -> {
                timestamp.ifPresent(e::setTimestamp);
                eventDecoder(kafkaRecord, e);
            });
            if (isInterrupted()) {
                consumer.wakeup();
                break;
            }
        }
    }

    private void eventDecoder(ConsumerRecord<byte[], byte[]> kafkaRecord, Event e) {
        getHeaders(kafkaRecord).forEach(e::putMeta);
        e.putMeta("kafka_topic", kafkaRecord.topic());
        e.putMeta("kafka_partition", kafkaRecord.partition());
        byte[] keyBytes = kafkaRecord.key();
        if (keyClass != null) {
            try {
                e.putMeta("kafka_key", Expression.convertObject(keyClass, keyBytes, StandardCharsets.UTF_8, ByteOrder.LITTLE_ENDIAN));
            } catch (UnknownHostException | InvocationTargetException ex) {
                logger.atWarn().withThrowable(logger.isDebugEnabled() ? ex : null).log("Unable to decode key: {}", () -> Helpers.resolveThrowableException(ex));
            }
        } else if (kafkaRecord.headers().lastHeader(KeyTypes.HEADER_NAME) != null){
            byte[] keyTypeHeaderValue = kafkaRecord.headers().lastHeader(KeyTypes.HEADER_NAME).value();
            if (keyTypeHeaderValue.length == 1) {
                byte keyType = keyTypeHeaderValue[0];
                try {
                    e.putMeta("kafka_key", KeyTypes.getById(keyType).read(keyBytes));
                } catch (IllegalArgumentException ex) {
                    e.putMeta("kafka_key", keyBytes);
                    e.putMeta("kafka_keyType", keyType);
                }
            } else {
                e.putMeta("kafka_key", keyBytes);
                e.putMeta("kafka_keyType", keyTypeHeaderValue);
            }
        }
        send(e);
    }

    private Map<String, byte[]> getHeaders(ConsumerRecord<byte[], byte[]> kafakRecord) {
        Header[] h = kafakRecord.headers().toArray();
        if (h.length > 0) {
            Map<String, byte[]> headersMap = HashMap.newHashMap(h.length);
            Arrays.stream(h)
                  .filter(e -> ! KeyTypes.HEADER_NAME.equals(e.key()))
                  .forEach( i-> headersMap.put(i.key(), i.value()));
            return headersMap;
        } else {
            return Map.of();
        }
    }

    private RangeCollection getPartitionRange(int partition) {
        return ranges.computeIfAbsent(partition, k -> new RangeCollection());
    }

}
