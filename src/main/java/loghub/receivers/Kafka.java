package loghub.receivers;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
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
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Receiver;

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

    public Kafka(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
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
                Event event = emptyEvent(ctxt);
                if (record.timestampType() ==  TimestampType.CREATE_TIME) {
                    event.setTimestamp(new Date(record.timestamp()));
                }
                Header[] headers = record.headers().toArray();
                if (headers.length > 0) {
                    Map<String, byte[]> headersMap = new HashMap<>(headers.length);
                    Arrays.stream(headers).forEach( i-> headersMap.put(i.key(), i.value()));
                    event.put("headers", headersMap);
                }
                byte[] content = record.value();
                try {
                    event.putAll(decoder.decode(ctxt, content, 0, content.length));
                    send(event);
                } catch (DecodeException e) {
                    logger.error(e.getMessage());
                    logger.catching(e);
                }
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
