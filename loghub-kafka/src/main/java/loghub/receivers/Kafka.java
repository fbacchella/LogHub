package loghub.receivers;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
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

    @Setter
    public static class Builder extends Receiver.Builder<Kafka, Kafka.Builder> {
        private String[] brokers = new String[] { "localhost"};
        private int port = 9092;
        private String topic;
        private String group ="loghub";
        private String keyDeserializer = ByteArrayDeserializer.class.getName();
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

    private final String[] brokers;
    @Getter
    private final String topic;
    private Supplier<Consumer<Long, byte[]>> consumerSupplier;

    protected Kafka(Builder builder) {
        super(builder);
        this.brokers = Arrays.copyOf(builder.brokers, builder.brokers.length);
        this.topic = builder.topic;
        if (builder.consumer != null) {
            consumerSupplier = () -> builder.consumer;
        } else {
            consumerSupplier = getConsumer(builder);
        }
    }

    private Supplier<Consumer<Long,byte[]>> getConsumer(Builder builder) {
        Map<String, Object> props = new HashMap<>();
        URI[] brokersUrl = Helpers.stringsToUri(builder.brokers, builder.port, "http", logger);
        String resolvedBrokers = Arrays.stream(brokersUrl)
                                       .map( i -> i.getHost() + ":" + i.getPort())
                                       .collect(Collectors.joining(","));
        try {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resolvedBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, builder.group);
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
            boolean broke = false;
            Duration pollingInterval = Duration.ofMillis(100);
            while (! isInterrupted()) {
                ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(pollingInterval);
                if (consumerRecords.count() == 0) {
                    continue;
                }
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
                        consumer.commitSync(Collections.singletonMap(new TopicPartition(kafakRecord.topic(), kafakRecord.partition()), new OffsetAndMetadata(kafakRecord.offset())));
                        broke = true;
                        break;
                    }
                }
                if (! broke) {
                    consumer.commitAsync();
                } else {
                    break;
                }
            }
        }
        close();
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

    public String[] getBrokers() {
        return Arrays.copyOf(brokers, brokers.length);
    }

}
