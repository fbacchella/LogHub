package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.logstash.beats.Protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Filter;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import loghub.metrics.Stats;
import loghub.netty.transport.POLLER;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

public class TestBeats {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Beats", "org.logstash.beats");
    }

    private final ObjectWriter writer;

    private Beats receiver;
    private int port;
    private PriorityBlockingQueue queue;

    public TestBeats() {
        writer = JacksonBuilder.get(JsonMapper.class).getMapper().writer();
    }

    @Test(timeout = 5000)
    public void testSimple() throws IOException, InterruptedException {
        try {
            makeReceiver(i -> { /* */ }, Collections.emptyMap());
            List<Map<?, ?>> batch = Collections.singletonList(Collections.singletonMap("message", "LogHub"));
            sendFrame(encode(batch), new Socket());
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(e);
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (receiver != null) {
                receiver.stopReceiving();
            }
            throw e;
        }
    }

    @Test(timeout = 5000)
    public void testGarbage() throws IOException, InterruptedException {
        try {
            makeReceiver(i -> { /* */ }, Collections.emptyMap());
            sendFrame(ByteBuffer.allocate(100), new Socket());
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNull(e);
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (receiver != null) {
                receiver.stopReceiving();
            }
            throw e;
        }
    }

    @Test(timeout = 5000)
    public void testOversizedBatch() throws IOException, InterruptedException {
        try {
            makeReceiver(i -> {/* */ }, Collections.emptyMap());
            ByteBuffer out = ByteBuffer.allocate(4096);
            out.put(Protocol.VERSION_2);
            out.put(Protocol.CODE_WINDOW_SIZE);
            out.putInt(2);
            // First message
            out.put(Protocol.VERSION_2);
            out.put(Protocol.CODE_JSON_FRAME);
            out.putInt(1);
            out.putInt(Integer.MAX_VALUE);
            out.putInt(0);
            out.flip();
            sendFrame(out, new Socket());
            List<Map<?, ?>> batch = Collections.singletonList(Collections.singletonMap("message", "LogHub"));
            sendFrame(encode(batch), new Socket());
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(e);
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (receiver != null) {
                receiver.stopReceiving();
            }
            throw e;
        }
    }

    @Test(timeout = 5000)
    public void testSSL() throws IOException, InterruptedException {
        SslContextBuilder builder = SslContextBuilder.getBuilder();
        builder.setTrusts(Tools.getDefaultKeyStore());
        SSLContext sslctx = builder.build();
        try {
            makeReceiver(i -> {
                i.setSslContext(sslctx);
                i.setWithSSL(true);
                // It should be required for a better test, needs to understand how to make client side TLS works
                i.setSSLClientAuthentication(ClientAuthentication.WANTED);
            }, Collections.emptyMap());
            List<Map<?, ?>> batch = Collections.singletonList(Collections.singletonMap("message", "LogHub"));
            Socket s = sslctx.getSocketFactory().createSocket();
            sendFrame(encode(batch), s);
            Event e = queue.poll(6, TimeUnit.SECONDS);
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (receiver != null) {
                receiver.stopReceiving();
            }
            throw e;
        }
    }

    @Test
    public void testAlreadyBinded() throws IOException {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             Beats r = getReceiver(InetAddress.getLoopbackAddress().getHostAddress(), ss.getLocalPort())) {
            PriorityBlockingQueue receiver = new PriorityBlockingQueue();
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Assert.assertFalse(r.configure(new Properties(Collections.emptyMap())));
        }
    }

    private void sendFrame(ByteBuffer buf, Socket s) throws IOException {
        try (Socket socket = s) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            OutputStream os = socket.getOutputStream();
            os.write(buf.array(), buf.arrayOffset(), buf.remaining());
            os.flush();
        }

    }

    private void makeReceiver(Consumer<Beats.Builder> prepare, Map<String, Object> propsMap) {
        Properties props = new Properties(propsMap);
        port = Tools.tryGetPort();
        queue = new PriorityBlockingQueue();
        Beats.Builder builder = Beats.getBuilder();
        builder.setPort(port);
        builder.setEventsFactory(factory);
        prepare.accept(builder);

        receiver = builder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testtcplinesstream", null));
        Stats.registerReceiver(receiver);
        Assert.assertTrue(receiver.configure(props));
        receiver.start();
    }

    private Beats getReceiver(String host, int port) {
        Beats.Builder builder = Beats.getBuilder();
        builder.setHost(host);
        builder.setPort(port);
        return builder.build();
    }

    private ByteBuffer encode(List<Map<?, ?>> batch) throws JsonProcessingException {
        ByteBuffer out = ByteBuffer.allocate(4096);
        out.put(Protocol.VERSION_2);
        out.put(Protocol.CODE_WINDOW_SIZE);
        out.putInt(batch.size());

        // Aggregates the payload that we could decide to compress or not.
        for (int i = 0; i < batch.size(); i++) {
            encodeMessageWithJson(out, i, batch.get(i));
        }
        out.flip();
        return out;
    }

    private void encodeMessageWithJson(ByteBuffer payload, int sequence, Map<?, ?> message) throws JsonProcessingException {
        payload.put(Protocol.VERSION_2);
        payload.put(Protocol.CODE_JSON_FRAME);
        payload.putInt(sequence);

        byte[] json = writer.writeValueAsBytes(message);
        payload.putInt(json.length);
        payload.put(json);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Beats"
                              , BeanInfo.build("timeStampField", String.class)
                              , BeanInfo.build("filter", Filter.class)
                              , BeanInfo.build("poller", POLLER.class)
                              , BeanInfo.build("workerThreads", Integer.TYPE)
                              , BeanInfo.build("port", Integer.TYPE)
                              , BeanInfo.build("host", String.class)
                              , BeanInfo.build("rcvBuf", Integer.TYPE)
                              , BeanInfo.build("sndBuf", Integer.TYPE)
                              , BeanInfo.build("backlog", Integer.TYPE)
                              , BeanInfo.build("user", String.class)
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("jaasName", String.class)
                              , BeanInfo.build("withSSL", Boolean.TYPE)
                              , BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                              , BeanInfo.build("SSLKeyAlias", String.class)
                              , BeanInfo.build("clientInactivityTimeoutSeconds", Integer.TYPE)
                              , BeanInfo.build("maxPayloadSize", Integer.TYPE)
                              , BeanInfo.build("workers", Integer.TYPE)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
