package loghub.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

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
import org.apache.logging.log4j.Logger;

import loghub.ConnectionContext;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;

public class KafkaReceiver extends Receiver implements KafkaProperties {

    public static class KafkaContext implements ConnectionContext {
        public final String topic;
        public final int partition;
        public final long offset;
        private boolean acknowledged = false;
        private final Consumer<Long, byte[]> consumer;
        KafkaContext(Consumer<Long, byte[]> consumer, ConsumerRecord<Long, byte[]> record) {
            this.topic = record.topic();
            this.partition = record.partition();
            this.offset = record.offset();
            this.consumer = consumer;
        }
        @Override
        public void acknowledge() {
            if (! acknowledged) {
                consumer.commitAsync(Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(offset)),
                        (i,j) -> {acknowledged = true;});
            }
        }
    }

    private Consumer<Long, byte[]> consumer;

    // Beans
    private String[] brokers = new String[] { "localhost" };
    private int port = 9092;
    private String topic;
    private String group ="loghub";
    private String keyDeserializer = ByteArrayDeserializer.class.getName();
    private final Map<String, Object> genericProperties = new HashMap<>();

    public KafkaReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public String getReceiverName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

    @Override
    public boolean configure(loghub.configuration.Properties properties) {
        Properties props = configureKafka(properties);
        props.putAll(props);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        genericProperties.clear();
        consumer = new KafkaConsumer<>(props);
        return super.configure(properties);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        while (! isInterrupted()) {
            ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(100);
            if (consumerRecords.count() == 0) {
                continue;
            }
            for(ConsumerRecord<Long, byte[]> record: consumerRecords) {
                ConnectionContext ctxt = new KafkaContext(consumer, record);
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
            }
        }
        consumer.close();
    }

    @Override
    public void close() {
        super.close();
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

    @Override
    public Map<String, Object> getProperties() {
        return genericProperties;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public int getRequiredNumAcks() {
        return -1;
    }

    @Override
    public int getRetries() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getKerb5ConfPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getClientJaasConfPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSaslKerberosServiceName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystorePassword() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystoreLocation() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystoreType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslTruststorePassword() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslTruststoreLocation() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSecurityProtocol() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCompressionType() {
        // TODO Auto-generated method stub
        return null;
    }

}
