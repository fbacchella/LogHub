package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.events.Event;

public class TestKafka {

    private static Logger logger;

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
        builder.setBrokers(new String[] {"192.168.0.13"});
        builder.setTopic("test");
        builder.setDecoder(StringCodec.getBuilder().build());
        MockConsumer<byte[], byte[]> mockConsumer = getMockConsumer();
        builder.setConsumer(mockConsumer);

        try (Kafka r = builder.build()) {
            PriorityBlockingQueue queue = new PriorityBlockingQueue();
            r.setOutQueue(queue);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testkafka", null));
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            r.start();

            mockConsumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, "messagebody".getBytes(StandardCharsets.UTF_8)));
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertEquals("messagebody", e.get("message"));
            System.err.println(e);
        }
    }

    private MockConsumer<byte[], byte[]> getMockConsumer() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(List.of(partition));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(partition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        return consumer;
    }

}
