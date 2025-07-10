package loghub.senders;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.kafka.KafkaProperties;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Kafka.Builder.class)
@AsyncSender
public class Kafka extends Sender {

    private static class KafkaKeySerializer implements Serializer<Event> {
        private final Expression keySerializer;

        private KafkaKeySerializer(Expression keySerializer) {
            this.keySerializer = keySerializer;
        }

        /**
         * Convert {@code data} into a byte array.
         *
         * @param topic topic associated with data
         * @param data  typed data
         * @return serialized bytes
         */
        @Override
        public byte[] serialize(String topic, Event data) {
            try {
                return keySerializer.eval(data, topic).toString().getBytes(StandardCharsets.UTF_8);
            } catch (ProcessorException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Setter @Getter
    public static class Builder extends Sender.Builder<Kafka> implements KafkaProperties {
        private String[] brokers = new String[] {"localhost"};
        private int port = 9092;
        private String topic;
        private String group = "loghub";
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuthentication;
        private String securityProtocol;
        private String saslKerberosServiceName;
        private Expression keySerializer = new Expression("random", ed -> Math.random());
        private String compressionType;
        private int retries = -1;
        private String acks;
        private int linger = -1;
        // To be removed once heritage problem is solved
        private int batchSize = -1;

        // Only used for tests
        @Setter(AccessLevel.PACKAGE)
        private Producer<Event, byte[]> producer;
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
    private Supplier<Producer<Event, byte[]>> producerSupplier;
    private Producer<Event, byte[]> producer;

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
    public void run() {
        producer = producerSupplier.get();
        producerSupplier = null;
        try {
            super.run();
        } finally {
            Optional.ofNullable(producer).ifPresent(Producer::close);
        }
    }

    private Supplier<Producer<Event, byte[]>> getProducer(Kafka.Builder builder) {
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
        if (builder.linger > 0) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, builder.linger);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, builder.keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, builder.batchSize);

        return () -> new KafkaProducer<>(props, new KafkaKeySerializer(builder.keySerializer), new ByteArraySerializer());
    }

    @Override
    protected boolean send(Event event) throws EncodeException {
        ProducerRecord<Event, byte[]> kRecord = new ProducerRecord<>(topic, null, event.getTimestamp().getTime(), event, encode(event));
        producer.send(kRecord, (m, ex) -> {
            if (ex != null) {
                // All kafka exception are handled as IO exception, logged without stack unless requested
                if (ex instanceof KafkaException) {
                    Stats.failedSentEvent(this, ex, event);
                    logger.atError()
                          .withThrowable(logger.isDebugEnabled() ? ex : null)
                          .log("Sending exception: {}", Helpers.resolveThrowableException(ex));
                } else {
                    handleException(ex, event);
                }
            } else {
                processStatus(event, true);
            }
        });
        return true;
    }

    @Override
    public String getSenderName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

}
