package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;
import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.ProtobufTestUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;
import prometheus.Remote;

public class TestPrometheus {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers", "loghub.netty", "loghub.EventsProcessor", "loghub.security");
    }

    private Prometheus receiver = null;
    private PriorityBlockingQueue queue;
    private String hostname;
    private int port;

    private Prometheus makeReceiver(Consumer<Prometheus.Builder> prepare, Map<String, Object> propsMap) {
        Properties props = new Properties(propsMap);
        // Generate a locally bound random socket
        port = Tools.tryGetPort();
        hostname = InetAddress.getLoopbackAddress().getCanonicalHostName();

        queue = new PriorityBlockingQueue();

        Prometheus.Builder httpbuilder = Prometheus.getBuilder();
        httpbuilder.setHost(hostname);
        httpbuilder.setPort(port);
        httpbuilder.setEventsFactory(factory);
        prepare.accept(httpbuilder);

        receiver = httpbuilder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testhttp", null));
        Stats.registerReceiver(receiver);
        Assert.assertTrue(receiver.configure(props));
        receiver.start();
        return receiver;
    }

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private void doRequest(Message msg) throws IOException, InterruptedException {
        Snappy snappy = new Snappy();
        ByteBuf content = Unpooled.wrappedBuffer(msg.toByteArray());
        ByteBuf compressedBuffer = Unpooled.buffer(content.readableBytes() * 2, content.readableBytes() * 2);
        snappy.encode(content, compressedBuffer, content.readableBytes());
        HttpClient client = HttpClient.newBuilder()
                                    .connectTimeout(Duration.ofSeconds(5))
                                    .build();
        java.net.http.HttpRequest.Builder jRequestBuilder = java.net.http.HttpRequest.newBuilder();
        HttpRequest req = jRequestBuilder.method("POST", java.net.http.HttpRequest.BodyPublishers.ofByteArray(compressedBuffer.array(), 0, compressedBuffer.readableBytes()))
                                  .uri(URI.create(String.format("http://%s:%d/api/v1/write", hostname, port)))
                                  .header("Content-Type", "application/x-protobuf")
                                  .header("Content-Encoding", "snappy")
                                  .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                                  .build();
        HttpResponse<?> response = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        Assert.assertEquals(200, response.statusCode());
    }

    @Test(timeout = 5000)
    public void testSimpleSend() throws IOException, InterruptedException {
        Remote.WriteRequest wr = ProtobufTestUtils.getWriteRequest();
        try (Prometheus receiver = makeReceiver( i -> {}, Collections.emptyMap())) {
            doRequest(wr);
        }
        Event ev = queue.poll();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> timeseries = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("timeseries"));
        Assert.assertEquals(1, timeseries.size());
        Map<String, Object> timeserie = timeseries.get(0);
        Assert.assertEquals("test_event", timeserie.get("name"));
        Assert.assertEquals(Map.of("label", "value"), timeserie.get("labels"));
        Assert.assertEquals(Map.of("value", 1.0, "timestamp", Instant.ofEpochMilli(1)), timeserie.get("sample"));
    }

    @Test
    public void test_loghub_receivers_Prometheus() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Prometheus"
                , BeanInfo.build("useJwt", Boolean.TYPE)
                , BeanInfo.build("user", String.class)
                , BeanInfo.build("password", String.class)
                , BeanInfo.build("jaasName", String.class)
                , BeanInfo.build("withSSL", Boolean.TYPE)
                , BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                , BeanInfo.build("SSLKeyAlias", String.class)
                , BeanInfo.build("backlog", Integer.TYPE)
                , BeanInfo.build("sndBuf", Integer.TYPE)
                , BeanInfo.build("rcvBuf", Integer.TYPE)
                , BeanInfo.build("blocking", Boolean.TYPE)
        );
    }

}
