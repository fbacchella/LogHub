package loghub.senders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.ZMQSink;
import loghub.configuration.Properties;
import loghub.encoders.ToJson;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import zmq.socket.Sockets;

public class TestZMQSender {

    private static Logger logger;

    @Rule(order=1)
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule(order=2)
    public ZMQFactory tctxt = new ZMQFactory(testFolder, "secure");

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.ZMQSink", "loghub.senders.ZMQ", "loghub.ContextRule");
    }

    private CountDownLatch latch;
    private final StringBuffer received = new StringBuffer();

    public String process(Socket socket) {
        String in = socket.recvStr();
        if (in != null) {
            received.append(in);
            latch.countDown();
        }
        return in;
    }

    private void dotest(Consumer<ZMQ.Builder> configure, Consumer<ZMQSink.Builder<String>> sinkconfigure, String pattern)
                    throws IOException, InterruptedException, ZMQCheckedException {
        received.setLength(0);
        ZMQSocketFactory ctx = tctxt.getFactory();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();

        ZMQSink.Builder<String> sinkbuilder = ZMQSink.getBuilder();
        sinkbuilder.setReceive(this::process)
                    .setZmqFactory(ctx)
                    .setSource(rendezvous)
                    .setType(SocketType.PULL);
        sinkconfigure.accept(sinkbuilder);

        BlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);
        AtomicInteger count = new AtomicInteger();
        Thread injector = ThreadBuilder.get().setTask(() -> {
            try {
                while (true) {
                    Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
                    ev.put("message", count.incrementAndGet());
                    queue.offer(ev);
                    Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).setDaemon(true).setName("EventSource").build(true);

        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setType(Sockets.PUSH.name());
        builder.setDestination(rendezvous);
        configure.accept(builder);

        Properties p = new Properties(Collections.singletonMap("zmq.keystore", Paths.get(testFolder.newFolder().getAbsolutePath(), "zmqtest.jks").toString()));
        try (ZMQSink<String> sink = sinkbuilder.build() ; ZMQ sender = builder.build()) {
            Thread.sleep(100);
            sender.setInQueue(queue);
            Assert.assertTrue(sender.configure(p));
            latch = new CountDownLatch(4);
            sender.start();
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        } finally {
            injector.interrupt();
        }
        p.zSocketFactory.close();
        String buf = received.toString();
        Assert.assertTrue(buf, Pattern.matches(pattern, buf));
   }

    @Test(timeout=5000)
    public void bind() throws IOException, InterruptedException, ZMQCheckedException {
         dotest(s ->  s.setMethod(Method.CONNECT.name()),
                s -> s.setMethod(Method.BIND),
                "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout=5000)
    public void connect() throws IOException, InterruptedException, ZMQCheckedException {
        dotest(s ->  s.setMethod(Method.BIND.name()),
               s -> s.setMethod(Method.CONNECT),
               "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout=5000)
    public void batch() throws IOException, InterruptedException, ZMQCheckedException {
        dotest((s) -> {
            s.setMethod(Method.CONNECT.name());
            s.setBatchSize(2);
        }, s -> s.setMethod(Method.BIND), "(\\[\\{\"message\":\\d+\\},\\{\"message\":\\d+\\}\\])+");
    }

//    @Test(timeout=5000)
//    public void curveClient() throws IOException, InterruptedException, ZMQCheckedException {
//        Path keyPubpath = Paths.get(testFolder.getRoot().getPath(), "secure", "zmqtest.pub");
//        String keyPub;
//        try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
//            Files.copy(keyPubpath, pubkeyBuffer);
//            keyPub = new String(pubkeyBuffer.toByteArray(), StandardCharsets.UTF_8);
//        }
//
//        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
//        ZMQSink.Builder flowbuilder = ZMQSink.getBuilder()
//                        .setMethod(Method.CONNECT)
//                        .setSource(rendezvous)
//                        .setSecurity("Curve")
//                        .setPrivateKey(serverKeys[1])
//                        .setPublicKey(serverKeys[0]);
//
//        dotest((s) -> {
//            s.setDestination(rendezvous);
//            s.setMethod(Method.BIND.name());
//            s.setServerKey("Curve "+ Base64.getEncoder().encodeToString(serverKeys[0]));
//        }, flowbuilder, Mechanisms.CURVE, "(\\{\"message\":\\d+\\})+");
//    }

    @Test(timeout=5000)
    public void curveServer() throws IOException, InterruptedException, ZMQCheckedException {
        Path keyPubpath = Paths.get(testFolder.getRoot().getPath(), "secure", "zmqtest.pub");
        String keyPub;
        try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
            Files.copy(keyPubpath, pubkeyBuffer);
            keyPub = new String(pubkeyBuffer.toByteArray(), StandardCharsets.UTF_8);
        }
        dotest(s -> {
            s.setMethod(Method.BIND.name());
            s.setServerKey(keyPub);
            s.setSecurity("Curve");
        },
        s -> s.setMethod(Method.CONNECT).setKeyEntry(tctxt.getFactory().getKeyEntry()).setServerKey(keyPub).setSecurity("Curve"),
        "(\\{\"message\":\\d+\\})+");
    }

    @Test
    public void testBeans() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.senders.ZMQ"
                              ,BeanInfo.build("method", String.class)
                              ,BeanInfo.build("destination", String.class)
                              ,BeanInfo.build("type", String.class)
                              ,BeanInfo.build("hwm", Integer.TYPE)
                              ,BeanInfo.build("serverKey", String.class)
                              ,BeanInfo.build("security", String.class)
                        );
    }

}
