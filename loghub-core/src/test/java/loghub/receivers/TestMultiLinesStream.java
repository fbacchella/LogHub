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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
import loghub.queue.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.netty.transport.POLLER;
import loghub.security.ssl.ClientAuthentication;

public class TestMultiLinesStream {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.MultiLinesStream", "loghub.netty", "loghub.EventsProcessor", "loghub.security", "loghub");
    }

    private int port;
    private PriorityBlockingQueue queue;

    @Test(timeout=5000)
    public void testSimple() throws IOException, InterruptedException {
        try (MultiLinesStream ignored = makeReceiver(i -> {}, Collections.emptyMap())){
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            Event e = queue.poll(1, TimeUnit.SECONDS).get();
            Assert.assertNotNull(e);
            String message = (String) e.get("message");
            Assert.assertEquals("LogHub", message);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @Test(timeout=5000)
    public void testZeroSeparator() throws IOException, InterruptedException {
        try (MultiLinesStream ignored = makeReceiver(i -> i.setSeparator("\0"), Collections.emptyMap())){
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub1\0LogHub2\0".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            for (Event e: List.of(queue.poll(1, TimeUnit.SECONDS).get(), queue.poll(1, TimeUnit.SECONDS).get())) {
                Assert.assertNotNull(e);
                String message = (String) e.get("message");
                Assert.assertTrue(message.startsWith("LogHub"));
                Assert.assertEquals("LogHub".length() + 1, message.length());
                Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            }
        }
    }

    @Test(timeout=5000)
    public void testWithMerge() throws IOException, InterruptedException {
        try (MultiLinesStream ignored = makeReceiver(i -> { }, Collections.emptyMap())){
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub1\n  details\nLogHub2\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            int i = 0;
            Event e;
            while ((e = queue.poll(1, TimeUnit.SECONDS).orElse(null)) != null) {
                Assert.assertNotNull(e);
                String message = (String) e.get("message");
                if (i == 0) {
                    Assert.assertEquals("LogHub1\ndetails", message);
                } else {
                    Assert.assertEquals("LogHub2", message);
                }
                Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
                i++;
            }
            Assert.assertEquals(2, i);
        }
    }

    @Test(timeout = 5000)
    public void testParse() throws ConfigException, IOException, InterruptedException {
        port = Tools.tryGetPort();
        String confile = "input {" +
                                 "    loghub.receivers.MultiLinesStream {\n" +
                                 "        separator: \"\\n\\00\",\n" +
                                 "        mergePattern: \"--(?<payload>.*)\",\n" +
                                 "        port: " + port + ",\n" +
                                 "        joinWith: \": \",\n" +
                                 "    }" +
                                 "} | $main\n" +
                                 "pipeline[main] {}";
        Properties conf = Tools.loadConf(new StringReader(confile));
        try (MultiLinesStream r = (MultiLinesStream) conf.receivers.stream().findAny().orElseThrow();
             Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
            OutputStream os = socket.getOutputStream();
            os.write("LogHub1\n\0--details\n\0LogHub2\n\0".getBytes(StandardCharsets.UTF_8));
            os.flush();
            int i = 0;
            Event e;
            while ((e = conf.mainQueue.poll(1, TimeUnit.SECONDS).orElse(null)) != null) {
                Assert.assertNotNull(e);
                String message = (String) e.get("message");
                socket.close();
                if (i == 0) {
                    Assert.assertEquals("LogHub1: details", message);
                } else {
                    Assert.assertEquals("LogHub2", message);
                }
                Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
                i++;
            }
            Assert.assertEquals(2, i);
        }
    }

    private MultiLinesStream makeReceiver(Consumer<MultiLinesStream.Builder> prepare, Map<String, Object> propsMap) {
        port = Tools.tryGetPort();
        queue = new PriorityBlockingQueue();
        MultiLinesStream.Builder builder = MultiLinesStream.getBuilder();
        builder.setPort(port);
        prepare.accept(builder);

        MultiLinesStream receiver = builder.build();
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
    public void test_loghub_receivers_MultiLinesStream() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.MultiLinesStream"
                              , BeanInfo.build("maxLength", Integer.TYPE)
                              , BeanInfo.build("separator", String.class)
                              , BeanInfo.build("joinWith", String.class)
                              , BeanInfo.build("mergePattern", String.class)
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
