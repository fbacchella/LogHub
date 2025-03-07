package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
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

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Filter;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.decoders.Decoder;
import loghub.decoders.Json;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.netty.transport.POLLER;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

public class TestTcpLinesStream {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.TcpLinesStream", "loghub.netty", "loghub.EventsProcessor", "loghub.security");
    }

    private int port;
    private PriorityBlockingQueue queue;

    @Test(timeout = 5000)
    public void testSimple() throws IOException, InterruptedException {
        try (TcpLinesStream ignored = makeReceiver(i -> { }, Collections.emptyMap())) {
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(e);
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @Test(timeout = 5000)
    public void testZeroSeparator() throws IOException, InterruptedException {
        try (TcpLinesStream ignored = makeReceiver(i -> i.setSeparator("\0"), Collections.emptyMap())) {
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub1\0LogHub2\0".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            for (Event e : List.of(queue.poll(1, TimeUnit.SECONDS), queue.poll(1, TimeUnit.SECONDS))) {
                Assert.assertNotNull(e);
                String message = (String) e.get("message");
                Assert.assertTrue(message.startsWith("LogHub"));
                Assert.assertEquals("LogHub".length() + 1, message.length());
                Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            }
        }
    }

    @Test(timeout = 5000)
    public void testParse() throws ConfigException, IOException, InterruptedException {
        port = Tools.tryGetPort();
        String confile = "input {" +
                                 "    loghub.receivers.TcpLinesStream {\n" +
                                 "        separator: \"\\n\\00\",\n" +
                                 "        port: " + port + ",\n" +
                                 "    }" +
                                 "} | $main\n" +
                                 "pipeline[main] {}";
        Properties conf = Tools.loadConf(new StringReader(confile));
        try (TcpLinesStream r = (TcpLinesStream) conf.receivers.stream().findAny().orElseThrow();
             Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
            OutputStream os = socket.getOutputStream();
            os.write("LogHub1\n\0LogHub2\n\0".getBytes(StandardCharsets.UTF_8));
            os.flush();
            for (Event e : List.of(conf.mainQueue.poll(1, TimeUnit.SECONDS), conf.mainQueue.poll(1, TimeUnit.SECONDS))) {
                Assert.assertNotNull(e);
                String message = (String) e.get("message");
                Assert.assertTrue(message.startsWith("LogHub"));
                Assert.assertEquals("LogHub".length() + 1, message.length());
                Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            }
        }
    }

    @Test(timeout = 5000)
    public void testJson() throws IOException, InterruptedException {
        try (TcpLinesStream ignored = makeReceiver(i -> {}, Collections.emptyMap(), () -> Json.getBuilder().build())){
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("{\"program\": \"LogHub\"}\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            Event e = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(e);
            String message = (String) e.get("program");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @Test(timeout = 5000)
    public void testSSL() throws IOException, InterruptedException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("trusts", Tools.getDefaultKeyStore());
        SslContextBuilder builder = SslContextBuilder.getBuilder();
        builder.setTrusts(Tools.getDefaultKeyStore());
        SSLContext sslctx = builder.build();
        try (TcpLinesStream ignored = makeReceiver(i -> {
                    i.setSslContext(sslctx);
                    i.setWithSSL(true);
                    i.setSSLKeyAlias("localhost (loghub ca)");
                    i.setSSLClientAuthentication(ClientAuthentication.WANTED);
                },
                new HashMap<>(Collections.singletonMap("ssl.trusts", new String[] {getClass().getResource("/loghub.p12").getFile()}))
        )){
            try (Socket socket = sslctx.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            Event e = queue.poll(6, TimeUnit.SECONDS);
            Assert.assertEquals("CN=localhost", e.getConnectionContext().getPrincipal().getName());
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    private TcpLinesStream makeReceiver(Consumer<TcpLinesStream.Builder> prepare, Map<String, Object> propsMap) {
        return makeReceiver(prepare, propsMap, () -> StringCodec.getBuilder().build());
    }

    private TcpLinesStream makeReceiver(Consumer<TcpLinesStream.Builder> prepare, Map<String, Object> propsMap, java.util.function.Supplier<Decoder> decodsup) {
        port = Tools.tryGetPort();
        queue = new PriorityBlockingQueue();
        TcpLinesStream.Builder builder = TcpLinesStream.getBuilder();
        builder.setPort(port);
        builder.setDecoder(decodsup.get());
        prepare.accept(builder);

        TcpLinesStream receiver = builder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testtcplinesstream", null));
        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
        return receiver;
    }

    @Test
    public void testAlreadyBinded() throws IOException {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             TcpLinesStream r = getReceiver(InetAddress.getLoopbackAddress().getHostAddress(), ss.getLocalPort())) {
            PriorityBlockingQueue receiver = new PriorityBlockingQueue();
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Assert.assertFalse(r.configure(new Properties(Collections.emptyMap())));
        }
    }

    private TcpLinesStream getReceiver(String host, int port) {
        TcpLinesStream.Builder builder = TcpLinesStream.getBuilder();
        builder.setHost(host);
        builder.setPort(port);
        return builder.build();
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.TcpLinesStream"
                              , BeanInfo.build("maxLength", Integer.TYPE)
                              , BeanInfo.build("separator", String.class)
                              , BeanInfo.build("timeStampField", String.class)
                              , BeanInfo.build("filter", Filter.class)
                              , BeanInfo.build("poller", POLLER.class)
                              , BeanInfo.build("workerThreads", Integer.TYPE)
                              , BeanInfo.build("port", Integer.TYPE)
                              , BeanInfo.build("host", String.class)
                              , BeanInfo.build("rcvBuf", Integer.TYPE)
                              , BeanInfo.build("sndBuf", Integer.TYPE)
                              , BeanInfo.build("backlog", Integer.TYPE)
                              , BeanInfo.build("withSSL", Boolean.TYPE)
                              , BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                              , BeanInfo.build("SSLKeyAlias", String.class)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
