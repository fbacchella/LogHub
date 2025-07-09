package loghub.receivers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.FastExternalizeObject.Immutable;
import loghub.Helpers;
import loghub.kafka.KafkaProperties;
import loghub.kafka.range.RangeCollection;
import loghub.security.ssl.ClientAuthentication;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Blocking
@BuilderClass(Kafka.Builder.class)
public class Kafka extends Receiver<Kafka, Kafka.Builder> {

    @Immutable
    public static class KafkaContext extends ConnectionContext<Object> {
        public final String topic;
        private final Runnable onAcknowledge;
        KafkaContext(String topic, Runnable onAcknowledge) {
            this.topic = topic;
            this.onAcknowledge = onAcknowledge;
        }
        @Override
        public Object getLocalAddress() {
            return null;
        }
        @Override
        public Object getRemoteAddress() {
            return null;
        }
        @Override
        public void acknowledge() {
            super.acknowledge();
            onAcknowledge.run();
        }
    }

    @Setter @Getter
    public static class Builder extends Receiver.Builder<Kafka, Kafka.Builder> implements KafkaProperties {
        private String[] brokers = new String[] { "localhost"};
        private int port = 9092;
        private String topic;
        private String group ="loghub";
        private String compressionType;
        private String securityProtocol;
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuthentication;
        private String saslKerberosServiceName;
        // Only used for tests
        @Setter(AccessLevel.PACKAGE)
        private Consumer<Long, byte[]> consumer;
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
    private Supplier<Consumer<Long, byte[]>> consumerSupplier;
    private final RangeCollection range = new RangeCollection();

    protected Kafka(Builder builder) {
        super(builder);
        this.topic = builder.topic;
        if (builder.consumer != null) {
            consumerSupplier = () -> builder.consumer;
        } else {
            consumerSupplier = getConsumer(builder);
        }
    }

    private Supplier<Consumer<Long,byte[]>> getConsumer(Builder builder) {
        Map<String, Object> props = builder.configureKafka(logger);
        return () -> {
            Consumer<Long,byte[]> consumer = new KafkaConsumer<>(props, new LongDeserializer(), new ByteArrayDeserializer());
            consumer.subscribe(List.of(topic));
            return consumer;
        };
    }

    @Override
    public String getReceiverName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

    @Override
    public void run() {
        try (Consumer<Long, byte[]> consumer = consumerSupplier.get()) {
            consumerSupplier = null;
            Duration pollingInterval = Duration.ofMillis(100);
            while (! isInterrupted()) {
                try {
                    ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(pollingInterval);
                    if (consumerRecords.count() != 0) {
                        processRecords(consumer, consumerRecords);
                    }
                    commit(consumer);
                } catch (WakeupException e) {
                    break;
                }
            }
            commit(consumer);
        }
        close();
    }

    private void commit(Consumer<Long, byte[]> consumer) {
        long lastAck = range.merge();
        if (lastAck >= 0) {
            TopicPartition tp = new TopicPartition(this.topic, -1);
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(lastAck)), this::onComplete);
        }
    }

    private void onComplete(Map<TopicPartition,OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? exception : null)
                  .log("Failed commit {}: {}", () -> offsets, () -> Helpers.resolveThrowableException(exception));
        }
    }

    void processRecords(Consumer<Long, byte[]> consumer, ConsumerRecords<Long, byte[]> consumerRecords) {
        for (ConsumerRecord<Long, byte[]> kafakRecord: consumerRecords) {
            logger.trace("Got a record {}", kafakRecord);
            KafkaContext ctxt = new KafkaContext(kafakRecord.topic(), () -> range.addValue(kafakRecord.offset()));
            Optional<Date> timestamp = Optional.empty().map(ts ->  kafakRecord.timestampType() == TimestampType.CREATE_TIME ? new Date(kafakRecord.timestamp()) : null);
            byte[] content = kafakRecord.value();
            decodeStream(ctxt, content).forEach( e -> {
                timestamp.ifPresent(e::setTimestamp);
                getHeaders(kafakRecord).forEach(e::putMeta);
                e.putMeta("kafka_topic", kafakRecord.topic());
                e.putMeta("kafka_partition", kafakRecord.partition());
                send(e);
            });
            if (isInterrupted()) {
                consumer.wakeup();
                break;
            }
        }
    }

    private Map<String, byte[]> getHeaders(ConsumerRecord<Long, byte[]> kafakRecord) {
        Header[] h = kafakRecord.headers().toArray();
        if (h.length > 0) {
            Map<String, byte[]> headersMap = new HashMap<>(h.length);
            Arrays.stream(h).forEach( i-> headersMap.put(i.key(), i.value()));
            return headersMap;
        } else {
            return Map.of();
        }
    }

}
