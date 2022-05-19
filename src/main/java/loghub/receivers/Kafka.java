package loghub.receivers;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
import lombok.Getter;
import lombok.Setter;

@Blocking
@BuilderClass(Kafka.Builder.class)
public class Kafka extends Receiver {

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

    public static class Builder extends Receiver.Builder<Kafka> {
        @Setter
        private String[] brokers = new String[] { "localhost"};
        @Setter
        private int port = 9092;
        @Setter
        private String topic;
        @Setter
        private String group ="loghub";
        @Setter
        private String keyDeserializer = ByteArrayDeserializer.class.getName();
        @Override
        public Kafka build() {
            return new Kafka(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private Consumer<Long, byte[]> consumer;

    private final String[] brokers;
    @Getter
    private final int port;
    @Getter
    private final String topic;
    @Getter
    private final String group;
    @Getter
    private final String keyDeserializer;

    protected Kafka(Builder builder) {
        super(builder);
        this.brokers = Arrays.copyOf(builder.brokers, builder.brokers.length);
        this.port = builder.port;
        this.topic = builder.topic;
        this.group = builder.group;
        this.keyDeserializer = builder.keyDeserializer;
    }

    @Override
    public String getReceiverName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

    @Override
    public boolean configure(loghub.configuration.Properties properties) {
        Properties props = new Properties();
        URL[] brokersUrl = Helpers.stringsToUrl(brokers, port, "http", logger);
        String resolvedBrokers = Arrays.stream(brokersUrl)
                                       .map( i -> i.getHost() + ":" + i.getPort())
                                       .collect(Collectors.joining(","))
                                       ;
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resolvedBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        return super.configure(properties);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        boolean broke = false;
        while (! isInterrupted()) {
            ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(100);
            if (consumerRecords.count()==0) {
                continue;
            }
            for(ConsumerRecord<Long, byte[]> record: consumerRecords) {
                KafkaContext ctxt = new KafkaContext(record.topic());
                Optional<Date> timestamp = Optional.empty().map(ts ->  record.timestampType() ==  TimestampType.CREATE_TIME ? new Date(record.timestamp()) : null);
                Optional<Map<String, byte[]>> headers= Optional.empty().map(n-> {
                    Header[] h = record.headers().toArray();
                    if (h.length > 0) {
                        Map<String, byte[]> headersMap = new HashMap<>(h.length);
                        Arrays.stream(h).forEach( i-> headersMap.put(i.key(), i.value()));
                        return headersMap;
                    } else {
                        return null;
                    }
                });
                byte[] content = record.value();
                decodeStream(ctxt, content).forEach( e -> {
                    timestamp.ifPresent(e::setTimestamp);
                    headers.ifPresent( h -> e.put("headers", h));
                    send(e);
                });
                if (isInterrupted()) {
                    consumer.commitSync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset())));
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
        consumer.close();
        close();
    }

    public String[] getBrokers() {
        return Arrays.copyOf(brokers, brokers.length);
    }

}
