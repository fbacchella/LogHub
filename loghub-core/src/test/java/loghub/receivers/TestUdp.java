package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Consumer;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Compressor;
import loghub.Decompressor;
import loghub.Filter;
import loghub.FilterException;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.netty.transport.POLLER;

public class TestUdp {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Udp", "loghub.Receiver", "loghub.netty");
    }

    Udp receiver;

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private Udp getReceiver(Consumer<Udp.Builder> configure) {
        Udp.Builder b = Udp.getBuilder();
        b.setEventsFactory(factory);
        configure.accept(b);
        return b.build();
    }

    private void testsend(int size) throws IOException, InterruptedException {
        Properties props = new Properties(Collections.emptyMap());
        int port = Tools.tryGetPort();
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();

        // Generate a locally binded random socket
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress());
             Udp r = getReceiver(b -> {
                 b.setBufferSize(size + 10);
                 b.setHost(InetAddress.getLoopbackAddress().getHostAddress());
                 b.setPort(port);
                 b.setDecoder(StringCodec.getBuilder().build());
             })) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Stats.registerReceiver(r);
            String hostname = socket.getLocalAddress().getHostAddress();
            InetSocketAddress destaddr = new InetSocketAddress(hostname, port);

            Assert.assertTrue(r.configure(props));
            r.start();
            int originalMessageSize;
            try (DatagramSocket send = new DatagramSocket()) {
                StringBuilder buffer = new StringBuilder();
                while (buffer.length() <= size) {
                    buffer.append("message");
                }
                byte[] buf = buffer.toString().getBytes();
                originalMessageSize = buffer.length();
                DatagramPacket packet = new DatagramPacket(buf, buf.length, destaddr);
                try {
                    logger.debug("Listening on {}", r.getListen());
                    send.send(packet);
                    logger.debug("One message sent to {}", packet.getAddress());
                } catch (IOException e1) {
                    logger.error("IO exception on port {}", r.getPort());
                    throw e1;
                }
            }
            Event e = receiver.take();
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertTrue("Invalid message content", e.get("message").toString().startsWith("message"));
            Assert.assertEquals("Invalid message size", originalMessageSize, e.get("message").toString().length());
            Assert.assertTrue("didn't find valid remote host informations", e.getConnectionContext().getRemoteAddress() instanceof InetSocketAddress);
            Assert.assertTrue("didn't find valid local host informations", e.getConnectionContext().getLocalAddress() instanceof InetSocketAddress);
        }
    }

    @Test(timeout = 5000)
    public void testsmall() throws InterruptedException, IOException {
        testsend(1500);
    }

    @Test(timeout = 5000)
    public void testbig() throws InterruptedException, IOException {
        testsend(16384);
    }

    @Test(timeout = 5000)
    public void testCompressed() throws InterruptedException, IOException, FilterException {
        Properties props = new Properties(Collections.emptyMap());
        int port = Tools.tryGetPort();
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();

        Decompressor.Builder builder = Decompressor.getBuilder();

        Compressor.Builder cbuilder = Compressor.getBuilder();
        cbuilder.setFormat(CompressorStreamFactory.DEFLATE);
        Compressor comp = cbuilder.build();
        byte[] sentBuffer = comp.filter("Compressed message".getBytes(StandardCharsets.UTF_8));

        // Generate a locally binded random socket
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress());
             Udp r = getReceiver(b -> {
                 b.setBufferSize(4000);
                 b.setHost(InetAddress.getLoopbackAddress().getHostAddress());
                 b.setPort(port);
                 b.setDecoder(StringCodec.getBuilder().build());
                 b.setFilter(builder.build());
             })) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Stats.registerReceiver(r);
            String hostname = socket.getLocalAddress().getHostAddress();
            InetSocketAddress destaddr = new InetSocketAddress(hostname, port);

            Assert.assertTrue(r.configure(props));
            r.start();
            try (DatagramSocket send = new DatagramSocket()) {
                DatagramPacket packet = new DatagramPacket(sentBuffer, sentBuffer.length, destaddr);
                try {
                    logger.debug("Listening on {}", r.getListen());
                    send.send(packet);
                    logger.debug("One message sent to {}", packet.getAddress());
                } catch (IOException e1) {
                    logger.error("IO exception on port {}", r.getPort());
                    throw e1;
                }
            }
            Event e = receiver.take();
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertEquals("Invalid message content", "Compressed message", e.get("message"));
            Assert.assertTrue("didn't find valid remote host informations", e.getConnectionContext().getRemoteAddress() instanceof InetSocketAddress);
            Assert.assertTrue("didn't find valid local host informations", e.getConnectionContext().getLocalAddress() instanceof InetSocketAddress);
        }
    }

    @Test(timeout = 5000)
    public void testAlreadyBinded() throws IOException {
        try (DatagramSocket ss = new DatagramSocket(0, InetAddress.getLoopbackAddress());
             Udp r = getReceiver(b -> {
                 b.setHost(InetAddress.getLoopbackAddress().getHostAddress());
                 b.setPort(ss.getLocalPort());
                 b.setDecoder(StringCodec.getBuilder().build());
             })) {
            PriorityBlockingQueue receiver = new PriorityBlockingQueue();
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Assert.assertFalse(r.configure(new Properties(Collections.emptyMap())));
        }
    }

    @Test(timeout = 5000)
    public void testMultiThreads() {
        int port = Tools.tryGetPort();
        try (Udp r = getReceiver(b -> {
            b.setBufferSize(4000);
            b.setHost(InetAddress.getLoopbackAddress().getHostAddress());
            b.setPort(port);
            b.setWorkerThreads(4);
            b.setPoller(POLLER.DEFAULTPOLLER);
            b.setDecoder(StringCodec.getBuilder().build());
        })) {
            Assert.assertEquals(POLLER.DEFAULTPOLLER.isUnixSocket(), r.configure(new Properties(Collections.emptyMap())));
            r.start();
        }
    }

    @Test(timeout = 5000)
    public void testMultiThreadsFails() {
        int port = Tools.tryGetPort();
        try (Udp r = getReceiver(b -> {
            b.setBufferSize(4000);
            b.setHost(InetAddress.getLoopbackAddress().getHostAddress());
            b.setPort(port);
            b.setWorkerThreads(4);
            b.setPoller(POLLER.NIO);
            b.setDecoder(StringCodec.getBuilder().build());
        })) {
            // Failing because no unix socket available
            Assert.assertFalse(r.configure(new Properties(Collections.emptyMap())));
            r.start();
        }
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Udp"
                              , BeanInfo.build("host", String.class)
                              , BeanInfo.build("port", Integer.TYPE)
                              , BeanInfo.build("bufferSize", Integer.TYPE)
                              , BeanInfo.build("filter", Filter.class)
                              , BeanInfo.build("rcvBuf", Integer.TYPE)
                              , BeanInfo.build("sndBuf", Integer.TYPE)
                        );
    }

}
