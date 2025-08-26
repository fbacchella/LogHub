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
import java.util.concurrent.Semaphore;
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
import loghub.kafka.HeadersTypes;
import loghub.kafka.KafkaProperties;
import loghub.kafka.range.RangeCollection;
import loghub.security.ssl.ClientAuthentication;
import loghub.types.MimeType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Blocking
@BuilderClass(Kafka.Builder.class)
public class Kafka extends Receiver<Kafka, Kafka.Builder> {

    public static class KafkaContext extends BuildableConnectionContext<String> implements Cloneable {
        @Getter
        private final String topic;
        @Getter
        private final int partition;
        private final String remoteAddress;
        KafkaContext(String topic, int partition, Runnable onAcknowledge) {
            this.topic = topic;
            this.partition = partition;
            this.remoteAddress = String.format("%s/%d", topic, partition);
            setOnAcknowledge(onAcknowledge);
        }
        public Object clone() {
            KafkaContext kc = new KafkaContext(topic, partition, () -> {});
            kc.setPrincipal(getPrincipal());
            return kc;
        }
        @Override
        public String getLocalAddress() {
            return "";
        }

        @Override
        public String getRemoteAddress() {
            return remoteAddress;
        }

        @Override
        public Map<String, Object> getProperties() {
            return Map.of("topic", topic, "partition", partition);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getProperty(String property) {
            return switch (property) {
                case "topic" -> (Optional<T>) Optional.of(topic);
                case "partition" -> (Optional<T>) Optional.of(partition);
                default -> super.getProperty(property);
            };
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
        private int workers = 4;
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
    private final Semaphore workerThreads;
    private final Thread.Builder threadBuilder;

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
        workerThreads = new Semaphore(builder.workers);
        threadBuilder = Thread.ofVirtual().name(getReceiverName() + "RecordProcessor", 1);
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
        int wait = 100;
        while (! isInterrupted()) {
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
            try {
                // An exponential back off, that double on each step
                // and wait one hour max between each try
                Thread.sleep(wait);
                wait = Math.min(2 * wait, 3600 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean eventLoop(Consumer<byte[], byte[]> consumer, Duration pollingInterval) {
        try {
            ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(pollingInterval);
            if (consumerRecords.count() != 0) {
                logger.debug("Received {} records", consumerRecords::count);
                workerThreads.acquire();
                threadBuilder.start(() -> processRecords(consumer, consumerRecords));
            }
            commit(consumer);
            return true;
        } catch (WakeupException | InterruptException | InterruptedException e) {
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
        try {
            for (ConsumerRecord<byte[], byte[]> kafkaRecord: consumerRecords) {
                logger.trace("Got a record {}", kafkaRecord);
                int partition = kafkaRecord.partition();
                long offset = kafkaRecord.offset();
                KafkaContext ctxt = new KafkaContext(
                        kafkaRecord.topic(),
                        kafkaRecord.partition(),
                        () -> getPartitionRange(partition).addValue(offset)
                );
                Optional.ofNullable(kafkaRecord.headers().lastHeader(HeadersTypes.CONTENTYPE_HEADER_NAME))
                        .map(h -> MimeType.of(new String(h.value(), StandardCharsets.UTF_8)))
                        .map(decoders::get)
                        .ifPresent(ctxt::setDecoder);
                kafkaRecord.headers().remove(HeadersTypes.CONTENTYPE_HEADER_NAME);
                Optional<Date> timestamp = Optional.ofNullable(kafkaRecord.headers().lastHeader(HeadersTypes.DATE_HEADER_NAME))
                                                   .map(h -> new Date((long) HeadersTypes.LONG.read(h.value())))
                                                   .or(() -> Optional.ofNullable(kafkaRecord.timestampType() == TimestampType.CREATE_TIME ? new Date(kafkaRecord.timestamp()) : null));
                kafkaRecord.headers().remove(HeadersTypes.DATE_HEADER_NAME);
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
        } finally {
            workerThreads.release();
        }
    }

    private void eventDecoder(ConsumerRecord<byte[], byte[]> kafkaRecord, Event e) {
        getHeaders(kafkaRecord).forEach(e::putMeta);
        byte[] keyBytes = kafkaRecord.key();
        if (keyClass != null) {
            try {
                e.putMeta("kafka_key", Expression.convertObject(keyClass, keyBytes, StandardCharsets.UTF_8, ByteOrder.LITTLE_ENDIAN));
            } catch (UnknownHostException | InvocationTargetException ex) {
                logger.atWarn().withThrowable(logger.isDebugEnabled() ? ex : null).log("Unable to decode key: {}", () -> Helpers.resolveThrowableException(ex));
            }
        } else if (kafkaRecord.headers().lastHeader(HeadersTypes.KEYTYPE_HEADER_NAME) != null){
            byte[] keyTypeHeaderValue = kafkaRecord.headers().lastHeader(HeadersTypes.KEYTYPE_HEADER_NAME).value();
            if (keyTypeHeaderValue.length == 1) {
                byte keyType = keyTypeHeaderValue[0];
                try {
                    e.putMeta("kafka_key", HeadersTypes.getById(keyType).read(keyBytes));
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
                  .filter(e -> ! HeadersTypes.KEYTYPE_HEADER_NAME.equals(e.key()))
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
