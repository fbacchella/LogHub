package loghub.receivers;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.ContextLoader;

public class TestTcpLinesStream {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.TcpLinesStream", "loghub.netty", "loghub.EventsProcessor", "loghub.security");
    }

    private TcpLinesStream receiver;
    private int port;
    private BlockingQueue<Event> queue;

    @Test(timeout=5000)
    public void testSimple() throws IOException, InterruptedException {
        try {
            makeReceiver( i -> {}, Collections.emptyMap());
            try(Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
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

    @Test(timeout=5000)
    public void testSSL() throws IOException, InterruptedException {
        try {
            makeReceiver( i -> {
                i.setWithSSL(true);
                // It should be required for a better test, needs to understand how to make client side TLS works
                i.setSSLClientAuthentication(ClientAuthentication.WANTED.name());
            },
                          Collections.singletonMap("ssl.trusts", new String[] {getClass().getResource("/loghub.p12").getFile()})
                            );
            Map<String, Object> properties = new HashMap<>();
            properties.put("trusts", new String[] {getClass().getResource("/loghub.p12").getFile()});
            SSLContext cssctx = ContextLoader.build(properties);
            try(Socket socket = cssctx.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream os = socket.getOutputStream();
                os.write("LogHub\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
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

    private void makeReceiver(Consumer<TcpLinesStream> prepare, Map<String, Object> propsMap) {
        port = Tools.tryGetPort();
        queue = new ArrayBlockingQueue<>(1);
        receiver = new TcpLinesStream();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testtcplinesstream", null));
        receiver.setPort(port);
        receiver.setDecoder(StringCodec.getBuilder().build());
        prepare.accept(receiver);
        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
    }

    @Test
    public void testAlreadyBinded() throws IOException {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); TcpLinesStream r = new TcpLinesStream()) {
            BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(10);
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            r.setHost(InetAddress.getLoopbackAddress().getHostAddress());
            r.setPort(ss.getLocalPort());
            Assert.assertFalse(r.configure(new Properties(Collections.emptyMap())));
        }
    }

}
