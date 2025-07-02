package loghub.receivers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.kafka.KafkaProperties;
import loghub.security.ssl.ClientAuthentication;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Blocking
@BuilderClass(Kafka.Builder.class)
public class Kafka extends Receiver<Kafka, Kafka.Builder> {

    public static class KafkaContext extends ConnectionContext<Object> {
        public final String topic;
        KafkaContext(String topic) {
            this.topic = topic;
        }
        @Override
        public Object getLocalAddress() {
            return null;
        }
        @Override
        public Object getRemoteAddress() {
            return null;
        }
    }

    @Setter @Getter
    public static class Builder extends Receiver.Builder<Kafka, Kafka.Builder> implements KafkaProperties {
        private String[] brokers = new String[] { "localhost"};
        private int port = 9092;
        private String topic;
        private String group ="loghub";
        private String keyDeserializer = ByteArrayDeserializer.class.getName();
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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, builder.keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return () -> {
            Consumer<Long,byte[]> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
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
            AtomicReference<ConsumerRecord<Long, byte[]>> brokenRecordHolder = new AtomicReference<>();
            while (! isInterrupted()) {
                try {
                    ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(pollingInterval);
                    if (consumerRecords.count() == 0) {
                        continue;
                    }
                    processRecords(consumer, consumerRecords, brokenRecordHolder);
                } catch (WakeupException e) {
                    ConsumerRecord<Long, byte[]> brokenRecord = brokenRecordHolder.get();
                    consumer.commitSync(Collections.singletonMap(new TopicPartition(brokenRecord.topic(), brokenRecord.partition()), new OffsetAndMetadata(brokenRecord.offset())));
                    break;
                }
            }
        }
        close();
    }

    void processRecords(Consumer<Long, byte[]> consumer, ConsumerRecords<Long, byte[]> consumerRecords, AtomicReference<ConsumerRecord<Long, byte[]>> brokenRecordHolder) {
        for (ConsumerRecord<Long, byte[]> kafakRecord: consumerRecords) {
            logger.trace("Got a record {}", kafakRecord);
            KafkaContext ctxt = new KafkaContext(kafakRecord.topic());
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
                brokenRecordHolder.compareAndSet(null, kafakRecord);
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
