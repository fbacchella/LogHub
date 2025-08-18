package loghub.receivers;

import java.beans.IntrospectionException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.decoders.Json;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.kafka.HeadersTypes;
import loghub.security.ssl.ClientAuthentication;

public class TestKafka {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "org.apache.kafka", "loghub.receivers.Kafka");
    }

    @Test
    public void testone() throws InterruptedException {
        Kafka.Builder builder = Kafka.getBuilder();
        builder.setDecoder(StringCodec.getBuilder().build());
        builder.setDecoders(Map.of(
                "text/plain", StringCodec.getBuilder().build(),
                "text/json", Json.getBuilder().build()
        ));
        builder.setBrokers(new String[] {"192.168.0.13"});
        builder.setTopic("test");
        builder.setEventsFactory(factory);
        builder.setWithAutoCommit(false);
        MockConsumer<byte[], byte[]> mockConsumer = getMockConsumer();
        builder.setConsumer(mockConsumer);
        TopicPartition tp = new TopicPartition("test", 0);

        try (Kafka r = builder.build()) {
            PriorityBlockingQueue queue = new PriorityBlockingQueue();
            r.setOutQueue(queue);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testkafka", null));
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            r.start();

            mockConsumer.addRecord(getConsumerRecord(0, "text/plain", "messagebody".getBytes(StandardCharsets.UTF_8)));
            mockConsumer.addRecord(getConsumerRecord(1, "text/json", "{\"a\": 1}".getBytes(StandardCharsets.UTF_8)));
            mockConsumer.addRecord(getConsumerRecord(2, null, "offset2".getBytes(StandardCharsets.UTF_8)));
            Event e1 = queue.poll(1, TimeUnit.SECONDS);
            e1.getConnectionContext().acknowledge();
            Event e2 = queue.poll(1, TimeUnit.SECONDS);
            e2.getConnectionContext().acknowledge();
            queue.poll(1, TimeUnit.SECONDS);
            Assert.assertTrue(e1.getMetas().isEmpty());
            Assert.assertEquals("test", e1.getAtPath(VariablePath.parse("@context.topic")));
            Assert.assertEquals(0, e1.getAtPath(VariablePath.parse("@context.partition")));
            Assert.assertEquals("messagebody", e1.get("message"));
            Assert.assertEquals(0L, e1.getTimestamp().getTime());
            Assert.assertEquals("test", e1.getConnectionContext().getProperty("topic").orElseThrow());
            Assert.assertEquals(0, e1.getConnectionContext().getProperty("partition").orElseThrow());
            Assert.assertEquals("test/0", e1.getConnectionContext().getRemoteAddress());
            Assert.assertTrue(e2.getMetas().isEmpty());
            Assert.assertEquals(1, e2.getAtPath(VariablePath.of("a")));

            Assert.assertEquals(2, mockConsumer.committed(Set.of(tp)).get(tp).offset());
        }
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord(long offset, String contentType, byte[] body) {
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>("test", 0, offset, null, body);
        cr.headers().add(HeadersTypes.DATE_HEADER_NAME, HeadersTypes.LONG.write(0L));
        if (contentType != null) {
            cr.headers().add(HeadersTypes.CONTENTYPE_HEADER_NAME, HeadersTypes.STRING.write(contentType));
        }
        return cr;
    }

    private MockConsumer<byte[], byte[]> getMockConsumer() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        TopicPartition partition1 = new TopicPartition("test", 0);
        TopicPartition partition2 = new TopicPartition("test", 1);
        consumer.assign(List.of(partition1, partition2));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(partition1, 0L);
        beginningOffsets.put(partition2, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        return consumer;
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Kafka"
                , BeanInfo.build("brokers", String[].class)
                , BeanInfo.build("port", int.class)
                , BeanInfo.build("topic", String.class)
                , BeanInfo.build("group", String.class)
                , BeanInfo.build("keyClass", Class.class)
                , BeanInfo.build("classLoader", ClassLoader.class)
                , BeanInfo.build("sslContext", SSLContext.class)
                , BeanInfo.build("sslParams", SSLParameters.class)
                , BeanInfo.build("sslClientAuthentication", ClientAuthentication.class)
                , BeanInfo.build("securityProtocol", SecurityProtocol.class)
                , BeanInfo.build("withAutoCommit", boolean.class)
                , BeanInfo.build("kafkaProperties", Map.class)
                , BeanInfo.build("decoders", Map.class)
        );
    }

}
