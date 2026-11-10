package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Expression;
import loghub.LogUtils;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.encoders.ToJson;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.kafka.HeadersTypes;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;

public class TestKafka {

    private final EventsFactory factory = new EventsFactory();

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "org.apache.kafka", "loghub.senders.Kafka");
        Configurator.setLevel("org.apache.kafka.common.metrics", Level.INFO);
        Configurator.setLevel("org.apache.kafka.common.telemetry.internals", Level.INFO);
    }

    @Test
    public void testBuild() throws IOException {
        String configFile = "pipeline[kafka] {} output $kafka | { loghub.senders.Kafka { topic: \"test\", kafkaProperties: {\"a\": 1, \"b\": 2}, }}";
        Properties conf = Tools.loadConf(new StringReader(configFile));
        Sender[] senders = conf.senders.toArray(Sender[]::new);
        Kafka k = (Kafka) senders[0];
        Assert.assertEquals("test", k.getTopic());
    }

    @Test
    public void testone() throws InterruptedException {
        Properties props = new Properties(new HashMap<>(Map.of("__QUEUESDEPTH", 100 )));
        Kafka.Builder builder = Kafka.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setBrokers(new String[] {"192.168.0.13"});
        builder.setTopic("test");
        builder.setKeySerializer(new Expression("source", VariablePath.ofMeta("kafka_key")));
        MockProducer<byte[], byte[]> mockProducer = getMockProducer();
        builder.setProducerSupplier(() -> mockProducer);

        try (Kafka r = builder.build()) {
            PriorityBlockingQueue queue = new PriorityBlockingQueue();
            r.setInQueue(queue);
            Assert.assertTrue(r.configure(props));
            Stats.registerReceiver(r);
            r.start();
            BlockingConnectionContext ctx = new BlockingConnectionContext();
            Event ev = factory.newEvent(ctx);
            ev.putAtPath(VariablePath.of("a", "b"), 1);
            ev.putMeta("kafka_key", InetAddress.getLoopbackAddress());
            ev.setTimestamp(Instant.ofEpochMilli(Long.MAX_VALUE));
            queue.put(ev);
            mockProducer.flushed();
            Assert.assertTrue(ctx.lock.tryAcquire(5, TimeUnit.SECONDS));
            ProducerRecord<byte[], byte[]> kRecord = mockProducer.history().getFirst();
            Optional.ofNullable(kRecord.headers().lastHeader(HeadersTypes.KEYTYPE_HEADER_NAME))
                    .map(h -> HeadersTypes.getById(h.value()[0]))
                    .ifPresent(kt -> Assert.assertEquals(InetAddress.getLoopbackAddress(), kt.read(kRecord.key())));
            Assert.assertEquals("{\"a\":{\"b\":1}}", new String(kRecord.value(), StandardCharsets.UTF_8));
            Assert.assertEquals(Long.MAX_VALUE, HeadersTypes.LONG.read(kRecord.headers().lastHeader(HeadersTypes.DATE_HEADER_NAME).value()));
            Assert.assertEquals("application/json", HeadersTypes.STRING.read(kRecord.headers().lastHeader("Content-Type").value()));
        }
    }

    private MockProducer<byte[], byte[]> getMockProducer() {
        Serializer<byte[]> passThrough = new ByteArraySerializer();
        return new MockProducer<>(true, null, passThrough, passThrough);
    }

    @Test
    public void testBatchUsesOneProducerPerWorker() throws Exception {
        Properties props = new Properties(new HashMap<>(Map.of("__QUEUESDEPTH", 100)));
        Kafka.Builder builder = Kafka.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setBrokers(new String[] {"192.168.0.13"});
        builder.setTopic("test");
        builder.setBatchSize(5);
        builder.setWorkers(2);
        List<MockProducer<byte[], byte[]>> createdProducers = new ArrayList<>();
        builder.setProducerSupplier(() -> {
            MockProducer<byte[], byte[]> newProducer = getMockProducer();
            synchronized (createdProducers) {
                createdProducers.add(newProducer);
            }
            return newProducer;
        });

        Method flushMethod = Kafka.class.getDeclaredMethod("flush", Sender.Batch.class);
        flushMethod.setAccessible(true);
        try (Kafka r = builder.build()) {
            r.setInQueue(new PriorityBlockingQueue());
            Assert.assertTrue(r.configure(props));
            // Simulate two distinct batch worker threads, each flushing its own batch
            for (int i = 0; i < 2; i++) {
                Event ev = factory.newEvent();
                ev.putAtPath(VariablePath.of("a", "b"), i);
                ev.setTimestamp(Instant.ofEpochMilli(i));
                Sender.Batch batch = new Sender.Batch(r);
                batch.add(ev);
                Thread worker = new Thread(() -> {
                    try {
                        flushMethod.invoke(r, batch);
                    } catch (ReflectiveOperationException e) {
                        throw new AssertionError(e);
                    }
                });
                worker.start();
                worker.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
        // Each of the two simulated worker threads must resolve and keep its own producer
        Assert.assertEquals(2, createdProducers.size());
        Assert.assertNotSame(createdProducers.get(0), createdProducers.get(1));
        Assert.assertEquals(1, createdProducers.get(0).history().size());
        Assert.assertEquals(1, createdProducers.get(1).history().size());
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.Kafka"
                , BeanInfo.build("brokers", String[].class)
                , BeanInfo.build("port", int.class)
                , BeanInfo.build("topic", String.class)
                , BeanInfo.build("group", String.class)
                , BeanInfo.build("sslContext", SSLContext.class)
                , BeanInfo.build("sslParams", SSLParameters.class)
                , BeanInfo.build("sslClientAuthentication", ClientAuthentication.class)
                , BeanInfo.build("securityProtocol", SecurityProtocol.class)
                , BeanInfo.build("keySerializer", Expression.class)
                , BeanInfo.build("kafkaProperties", Map.class)
        );
    }

}
