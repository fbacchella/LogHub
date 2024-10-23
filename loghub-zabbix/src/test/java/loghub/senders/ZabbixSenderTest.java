package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.json.JsonMapper;

import fr.loghub.zabbix.ZabbixProtocol;
import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;

public class ZabbixSenderTest {

    private static class ZabbixServer extends Thread implements Thread.UncaughtExceptionHandler, AutoCloseable {

        private final CountDownLatch started = new CountDownLatch(0);
        private final Consumer<byte[]> queryProcessor;
        private final CompletableFuture<Boolean> doneProcessing = new CompletableFuture<>();
        private int port;

        public ZabbixServer(Consumer<byte[]> queryProcessor) throws InterruptedException {
            this.queryProcessor = queryProcessor;
            setUncaughtExceptionHandler(this);
            setName("ZabbixServer");
            start();
            started.await(1, TimeUnit.SECONDS);
        }

        public void run() {
            try (ServerSocketChannel server = ServerSocketChannel.open()){
                server.bind(new InetSocketAddress("127.0.0.1", 0));
                port = server.socket().getLocalPort();
                started.countDown();
                while (true) {
                    try (Socket client = server.accept().socket();
                            ZabbixProtocol handler = new ZabbixProtocol(client)
                    ) {
                        byte[] queryData = handler.read();
                        queryProcessor.accept(queryData);
                        handler.send("{\"response\":\"success\",\"info\":\"processed: 1; failed: 0; total: 1; seconds spent: 0.000052\"}".getBytes(StandardCharsets.UTF_8));
                   }
                }
            } catch (ClosedByInterruptException e) {
                doneProcessing.complete(true);
            } catch (IOException e) {
                doneProcessing.completeExceptionally(e);
            }
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            doneProcessing.completeExceptionally(e);
        }

        @Override
        public void close() {
            interrupt();
        }
    }
    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.ZabbixSender");
    }

    @Test
    public void sendEvent1() throws InterruptedException, IOException, EncodeException, SendException,
                                           ExecutionException {
        Map<String, ?> eventData = Map.of("value", "a", "key", "b");
        Map<?, ?> message = runTest(eventData, "value: [value], key: [key], host: \"host\", zabbixServer: \"localhost\"");
        List<Map<String, ?>> data = (List<Map<String, ?>>) message.get("data");
        Assert.assertEquals("host", data.get(0).get("host"));
        Assert.assertEquals("b", data.get(0).get("key"));
        Assert.assertEquals("a", data.get(0).get("value"));
    }

    @Test
    public void sendEvent2() throws InterruptedException, IOException, EncodeException, SendException,
                                            ExecutionException {
        Map<String, ?> eventData = Map.of("value", "a", "key", "b", "c", List.of(1, 2), "d", Instant.ofEpochMilli(0));
        Map<?, ?> message = runTest(eventData, "clock: [d], value: [value], key: [key], keyValues: [c], host: \"host\", zabbixServer: \"localhost\"");
        List<Map<String, ?>> data = (List<Map<String, ?>>) message.get("data");
        Assert.assertEquals("host", data.get(0).get("host"));
        Assert.assertEquals("b[1,2]", data.get(0).get("key"));
        Assert.assertEquals("a", data.get(0).get("value"));
        Assert.assertEquals(0, data.get(0).get("clock"));
        Assert.assertEquals(0, data.get(0).get("ns"));
    }

    @Test
    public void sendEvent3() throws InterruptedException, IOException, EncodeException, SendException,
                                            ExecutionException {
        Map<String, ?> eventData = Map.of("value", "a", "key", "b");
        Map<?, ?> message = runTest(eventData, "clock: now, value: [value], key: [key], host: \"host\", zabbixServer: \"localhost\"");
        List<Map<String, ?>> data = (List<Map<String, ?>>) message.get("data");
        Assert.assertEquals("host", data.get(0).get("host"));
        Assert.assertEquals("b", data.get(0).get("key"));
        Assert.assertEquals("a", data.get(0).get("value"));
    }

    private Map<?, ?> runTest(Map<String, ?> eventData, String configuration)
            throws InterruptedException, IOException, EncodeException, SendException, ExecutionException {
        EventsFactory factory = new EventsFactory();
        CompletableFuture<byte[]> queryProcessor = new CompletableFuture<>();
        try (ZabbixServer server = new ZabbixServer(queryProcessor::complete)) {
            Thread.sleep(100);

            String conf = String.format("pipeline[main] {} output $main | { loghub.senders.ZabbixSender {%s, port: %d}}", configuration, server.port);

            Properties p = Configuration.parse(new StringReader(conf));

            ZabbixSender sender = (ZabbixSender) p.senders.stream().findAny().get();
            Assert.assertTrue(sender.configure(p));
            Event ev = factory.newEvent();
            ev.putAll(eventData);
            sender.send(ev);
            sender.stopSending();
            byte[] query = queryProcessor.get();
            return JacksonBuilder.get(JsonMapper.class).getMapper().readValue(query, Map.class);
        }
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.ZabbixSender"
                , BeanChecks.BeanInfo.build("clock", Expression.class)
                , BeanChecks.BeanInfo.build("host", Expression.class)
                , BeanChecks.BeanInfo.build("key", Expression.class)
                , BeanChecks.BeanInfo.build("keyValues", Expression.class)
                , BeanChecks.BeanInfo.build("value", Expression.class)
                , BeanChecks.BeanInfo.build("fullEvent", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("port", Integer.TYPE)
                , BeanChecks.BeanInfo.build("zabbixServer", String.class)
                , BeanChecks.BeanInfo.build("connectTimeout", Integer.TYPE)
                , BeanChecks.BeanInfo.build("socketTimeout", Integer.TYPE)
                , BeanChecks.BeanInfo.build("sslContext", SSLContext.class)
                , BeanChecks.BeanInfo.build("withSsl", Boolean.TYPE)
        );
    }

}
