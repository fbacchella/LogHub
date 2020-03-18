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

import loghub.ConnectionContext;
import loghub.Helpers;

@Blocking
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

    private Consumer<Long, byte[]> consumer;

    // Beans
    private String[] brokers = new String[] { "localhost"};
    private int port = 9092;
    private String topic;
    private String group ="loghub";
    private String keyDeserializer = ByteArrayDeserializer.class.getName();

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
        return brokers;
    }

    public void setBrokers(String[] brokers) {
        this.brokers = brokers;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

}
