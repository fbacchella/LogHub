package loghub.senders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import loghub.events.Event;
import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.ZMQSink;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.ToJson;
import loghub.events.EventsFactory;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import zmq.io.mechanism.Mechanisms;
import zmq.socket.Sockets;

public class TestZMQSender {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

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

        Properties p = new Properties(Collections.singletonMap("zmq.keystore", Paths.get(testFolder.newFolder("server").getAbsolutePath(), "zmqtest.jks").toString()));

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
                    Event ev = factory.newEvent();
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
    public void batchConnect() throws IOException, InterruptedException, ZMQCheckedException {
        dotest((s) -> {
            s.setMethod(Method.CONNECT.name());
            s.setBatchSize(2);
        }, s -> s.setMethod(Method.BIND), "(\\[\\{\"message\":\\d+\\},\\{\"message\":\\d+\\}\\])+");
    }

    @Test(timeout=5000)
    public void batchBind() throws IOException, InterruptedException, ZMQCheckedException {
        dotest((s) -> {
            s.setMethod(Method.BIND.name());
            s.setBatchSize(2);
        }, s -> s.setMethod(Method.CONNECT), "(\\[\\{\"message\":\\d+\\},\\{\"message\":\\d+\\}\\])+");
    }

    private String getRemoteIdentity(String dir) {
        try {
            Path keyPubpath = Paths.get(testFolder.getRoot().getPath(), dir, "zmqtest.pub");
            try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
                Files.copy(keyPubpath, pubkeyBuffer);
                return new String(pubkeyBuffer.toByteArray(), StandardCharsets.UTF_8);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Test(timeout=5000)
    public void curveClient() throws IOException, InterruptedException, ZMQCheckedException {
        dotest(s -> {
            s.setMethod(Method.BIND.name());
            s.setSecurity(Mechanisms.CURVE);
            s.setServerKey(getRemoteIdentity("secure"));
        },
               s -> s.setMethod(Method.CONNECT).setKeyEntry(tctxt.getFactory().getKeyEntry()).setSecurity(Mechanisms.CURVE),
                        "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout=5000)
    public void curveServer() throws IOException, InterruptedException, ZMQCheckedException {
        dotest(s -> {
            s.setMethod(Method.BIND.name());
            s.setSecurity(Mechanisms.CURVE);
        },
               s -> s.setMethod(Method.CONNECT).setKeyEntry(tctxt.getFactory().getKeyEntry()).setServerKey(getRemoteIdentity("server")).setSecurity(Mechanisms.CURVE),
                        "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout=2000)
    public void testEncodeError() throws IOException, InterruptedException, EncodeException {
        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setType(Sockets.PUSH.name());
        builder.setDestination("tcp://localhost:" + Tools.tryGetPort());
        SenderTools.send(builder);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.ZMQ"
                              , BeanInfo.build("method", String.class)
                              , BeanInfo.build("destination", String.class)
                              , BeanInfo.build("type", String.class)
                              , BeanInfo.build("hwm", Integer.TYPE)
                              , BeanInfo.build("serverKey", String.class)
                              , BeanInfo.build("security", String.class)
                        );
    }

}
