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
import loghub.PriorityBlockingQueue;
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
    private PriorityBlockingQueue queue;

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
            SSLContext cssctx = ContextLoader.build(null, properties);
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

    private void makeReceiver(Consumer<TcpLinesStream.Builder> prepare, Map<String, Object> propsMap) {
        port = Tools.tryGetPort();
        queue = new PriorityBlockingQueue();
        TcpLinesStream.Builder builder = TcpLinesStream.getBuilder();
        builder.setPort(port);
        builder.setDecoder(StringCodec.getBuilder().build());
        prepare.accept(builder);
        
        receiver = new TcpLinesStream(builder);
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testtcplinesstream", null));
        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
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

}
