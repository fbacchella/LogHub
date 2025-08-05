package loghub.senders;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
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
@AsyncSender
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
        private Expression keySerializer = new Expression("random", ed -> ThreadLocalRandom.current().nextLong());
        private String compressionType;
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

    public Kafka(Builder builder) {
        super(builder);
        this.topic = builder.topic;
        this.keySerializer = builder.keySerializer;
        if (builder.producer != null) {
            producerSupplier = () -> builder.producer;
        } else {
            producerSupplier = getProducer(builder);
        }
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

    private Supplier<Producer<byte[], byte[]>> getProducer(Kafka.Builder builder) {
        Map<String, Object> props = builder.configureKafka(logger);
        return () -> new KafkaProducer<>(props, PASSTHROUGH_SERIALIZER, PASSTHROUGH_SERIALIZER);
    }

    @Override
    protected boolean send(Event event) throws EncodeException {
        ProducerRecord<byte[], byte[]> kRecord = getProducerRecord(event);
        producer.send(kRecord, (m, ex) -> {
            if (ex != null) {
                if (ex instanceof InterruptException) {
                    doInterrupt(ex);
                } else if (ex instanceof KafkaException) {
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

    private void doInterrupt(Exception ex) {
        logger.atWarn()
                .withThrowable(logger.isDebugEnabled() ? ex : null)
                .log("Interrupted");
        Thread.currentThread().interrupt();
    }

    private int waiting(int wait) {
        try {
            // An exponential back off, that double on each step
            // and wait one hour max between each try
            Thread.sleep(wait);
            return Math.min(2 * wait, 3600 * 1000);
        } catch (InterruptedException e) {
            logger.warn("Interrupted");
            Thread.currentThread().interrupt();
            return -1;
        }
    }

    @Override
    protected void flush(Batch documents) {
        boolean running;
        int wait = 1;
        do {
            try {
                running = true;
                for (PartitionInfo pi : producer.partitionsFor(topic)) {
                    if (pi.offlineReplicas().length >= pi.inSyncReplicas().length) {
                        logger.warn("A partition is failing: {}", () -> pi);
                        wait = waiting(wait);
                        running = false;
                        break;
                    }
                }
            } catch (InterruptException ex) {
                doInterrupt(ex);
                running = false;
                wait = -1;
            } catch (KafkaException ex) {
                running = false;
                logger.atError()
                      .withThrowable(logger.isDebugEnabled() ? ex : null)
                      .log("Checking status failed: {}", () -> Helpers.resolveThrowableException(ex));
            }
        } while (! running && wait > 0);
        // It was interrupted
        if (wait < 0) {
            documents.forEach(ef -> ef.complete(false));
        }
        documents.forEach(ef -> {
            Event event = ef.getEvent();
            try {
                ProducerRecord<byte[], byte[]> kRecord = getProducerRecord(event);
                producer.send(kRecord, (m, kex) -> {
                    if (kex instanceof InterruptException) {
                        doInterrupt(kex);
                        ef.complete(false);
                    } else if (kex instanceof KafkaException) {
                        Stats.failedSentEvent(this, kex, event);
                        logger.atError()
                              .withThrowable(logger.isDebugEnabled() ? kex : null)
                              .log("Sending exception: {}", () -> Helpers.resolveThrowableException(kex));
                    } else if (kex != null) {
                        ef.completeExceptionally(kex);
                    } else {
                        ef.complete(true);
                    }
                });
             } catch (EncodeException ex) {
                ef.completeExceptionally(ex);
            }
        });
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
        ProducerRecord<byte[], byte[]> kRecord = new ProducerRecord<>(topic, null, event.getTimestamp().getTime(), keyData, encode(event));
        if (keyClass >= 0) {
            kRecord.headers().add(KeyTypes.HEADER_NAME, new byte[]{keyClass});
        }
        kRecord.headers().add("Content-Type",  getEncoder().getMimeType().toString().getBytes(StandardCharsets.UTF_8));
        return kRecord;
    }

    @Override
    public String getSenderName() {
        return String.format("Kafka/%s/%s", topic, hashCode());
    }

}
