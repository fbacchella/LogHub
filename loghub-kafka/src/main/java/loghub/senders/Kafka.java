package loghub.senders;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.kafka.KafkaProperties;
import loghub.kafka.KeyTypes;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Kafka.Builder.class)
@CanBatch
public class Kafka extends Sender {

    private static final Serializer<byte[]> PASSTHROUGH_SERIALIZER = new ByteArraySerializer();

    @Setter @Getter
    public static class Builder extends Sender.Builder<Kafka> implements KafkaProperties {
        private String[] brokers = new String[] {"localhost"};
        private int port = 9092;
        private String topic = "LogHub";
        private String group = "LogHub";
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuthentication;
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private Expression keySerializer = new Expression(NullOrMissingValue.NULL);
        private Map<String, Object> kafkaProperties = Map.of();

        // Only used for tests
        @Setter(AccessLevel.PACKAGE)
        private Producer<byte[], byte[]> producer;
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
    private final Expression keySerializer;
    private Supplier<Producer<byte[], byte[]>> producerSupplier;
    private Producer<byte[], byte[]> producer;
    private final String senderName;

    public Kafka(Builder builder) {
        super(builder);
        this.topic = builder.topic;
        this.keySerializer = builder.keySerializer;
        int hash;
        if (builder.producer != null) {
            producerSupplier = () -> builder.producer;
            hash = builder.producer.hashCode();
        } else {
            Map<String, Object> props = builder.configureKafka(logger);
            producerSupplier = () -> new KafkaProducer<>(props, PASSTHROUGH_SERIALIZER, PASSTHROUGH_SERIALIZER);
            hash = Objects.hash(
                    props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                    props.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG),
                    props.get(CommonClientConfigs.CLIENT_ID_CONFIG),
                    props.get(CommonClientConfigs.GROUP_ID_CONFIG)
                    );
        }
        senderName = String.format("Kafka.%s@%s", topic, hash);
    }

    @Override
    public void run() {
        try {
            producer = producerSupplier.get();
            producerSupplier = null;
            super.run();
        } catch (InterruptException ex) {
            doInterrupt(ex);
        } catch (KafkaException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Kafka sender failed: {}", () -> Helpers.resolveThrowableException(ex));
        } finally {
            try {
                Optional.ofNullable(producer).ifPresent(Producer::close);
            } catch (org.apache.kafka.common.KafkaException ex) {
                logger.atWarn().withThrowable(logger.isDebugEnabled() ? ex : null).log("Kafka stop failed: {}", Helpers.resolveThrowableException(ex));
            }
        }
    }

    @Override
    protected boolean send(Event event) throws EncodeException {
        ProducerRecord<byte[], byte[]> kRecord = getProducerRecord(event);
        EventFuture ef = new EventFuture(event);
        producer.send(kRecord, (m, ex) -> documentCallback(ex, ef));
        try {
            return ef.get();
        } catch (InterruptedException | ExecutionException ex) {
            if (ex.getCause() instanceof InterruptException ie) {
                // Manual detection of the Kafka’s custom InterruptException
                doInterrupt(ie);
            } else {
                handleException(ex.getCause(), event);
            }
            return false;
        }
    }

    @Override
    protected void flush(Batch documents) {
        documents.forEach(ef -> {
            Event event = ef.getEvent();
            try {
                ProducerRecord<byte[], byte[]> kRecord = getProducerRecord(event);
                producer.send(kRecord, (m, kex) -> documentCallback(kex, ef));
            } catch (EncodeException ex) {
                ef.completeExceptionally(ex);
            }
        });
    }

    /**
     * Kafka interrupt is a runtime exception, needs special handling
     * @param ex
     */
    private void doInterrupt(Exception ex) {
        logger.atWarn()
                .withThrowable(logger.isDebugEnabled() ? ex : null)
                .log("Interrupted");
        Thread.currentThread().interrupt();
    }

    private void documentCallback(Exception ex, EventFuture ef) {
        Event event = ef.getEvent();
        if (ex instanceof InterruptException) {
            doInterrupt(ex);
        }if (ex instanceof KafkaException) {
            Stats.failedSentEvent(this, ex, event);
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Sending exception: {}", () -> Helpers.resolveThrowableException(ex));
        } else if (ex != null) {
            ef.completeExceptionally(ex);
        } else {
            ef.complete(true);
        }
    }

    private ProducerRecord<byte[], byte[]> getProducerRecord(Event event) throws EncodeException {
        byte[] keyData;
        byte keyClass;
        try {
            Object key = keySerializer.eval(event, topic);
            KeyTypes type = KeyTypes.resolve(key);
            keyData = type.write(key);
            keyClass = type.getId();
        } catch (IgnoredEventException e) {
            keyData = null;
            keyClass = -1;
        } catch (ProcessorException e) {
            throw new EncodeException("Key serialization failed", e);
        }
        ProducerRecord<byte[], byte[]> kRecord = new ProducerRecord<>(topic, null, null, keyData, encode(event));
        if (keyClass >= 0) {
            kRecord.headers().add(KeyTypes.HEADER_NAME, new byte[]{keyClass});
        }
        kRecord.headers().add("Date",  KeyTypes.LONG.write(event.getTimestamp().getTime()));
        kRecord.headers().add("Content-Type",  getEncoder().getMimeType().toString().getBytes(StandardCharsets.UTF_8));
        return kRecord;
    }

    @Override
    public String getSenderName() {
        return senderName;
    }

}
