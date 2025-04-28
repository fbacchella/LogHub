package loghub.senders;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.kafka.KafkaProperties;
import loghub.security.ssl.ClientAuthentication;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Kafka.Builder.class)
@CanBatch
public class Kafka extends Sender {

    @Setter @Getter
    public static class Builder extends Sender.Builder<Kafka> implements KafkaProperties {
        private String[] brokers = new String[] { "localhost"};
        private int port = 9092;
        private String topic;
        private String group = "loghub";
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuthentication;
        private String securityProtocol;
        private String saslKerberosServiceName;

        private String keySerializer = ByteArraySerializer.class.getName();
        private String compressionType = null;
        int retries = -1;
        String acks = null;

        // Only used for tests
        @Setter(AccessLevel.PACKAGE)
        private Producer<Long, byte[]> producer;
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
    private Supplier<Producer<Long, byte[]>> producerSupplier;
    private Producer<Long, byte[]> producer;

    public Kafka(Builder builder) {
        super(builder);
        this.topic = builder.topic;
        if (builder.producer != null) {
            producerSupplier = () -> builder.producer;
        } else {
            producerSupplier = getProducer(builder);
        }
    }

    @Override
    public boolean configure(Properties properties) {
        producer = producerSupplier.get();
        producerSupplier = null;
        return super.configure(properties);
    }

    private Supplier<Producer<Long,byte[]>> getProducer(Kafka.Builder builder) {
        Map<String, Object> props = builder.configureKafka(logger);
        if (builder.compressionType != null) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, builder.compressionType);
        }
        if (builder.acks != null) {
            props.put(ProducerConfig.ACKS_CONFIG, builder.acks);
        }
        if (builder.retries > 0) {
            props.put(ProducerConfig.RETRIES_CONFIG, builder.retries);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, builder.keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return () -> {
            Producer<Long,byte[]> producer = new KafkaProducer<>(props);
            return producer;
        };
    }

    @Override
    protected boolean send(Event event) throws SendException, EncodeException {
        try {
            ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, -1, event.getTimestamp().getTime(), null, encode(event));
            producer.send(record).get();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new SendException(e.getCause());
        }
    }

    @Override
    public String getSenderName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

}
