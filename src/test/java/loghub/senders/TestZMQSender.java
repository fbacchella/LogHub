package loghub.senders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQSink;
import loghub.configuration.Properties;
import loghub.encoders.ToJson;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper.Method;
import zmq.io.mechanism.curve.Curve;
import zmq.socket.Sockets;

public class TestZMQSender {

    private static Logger logger;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.ZMQSink", "loghub.senders.ZMQ", "loghub.ContextRule");
    }

    private CountDownLatch latch;

    public boolean process(Socket socket, int eventMask) {
        while ((socket.getEvents() & ZPoller.IN) != 0) {
            String received = socket.recvStr();
            latch.countDown();
            if (received == null) {
                Error error = Error.findByCode(socket.errno());
                logger.error("error with ZSocket {}: {}", socket, error.getMessage());
                return false;
            }
        }
        return true;
    }

    private void dotest(SmartContext ctx, Consumer<ZMQ.Builder> configure, ZMQSink.Builder sinkbuilder) throws IOException, InterruptedException {
        if (ctx == null) {
            ctx = SmartContext.getContext();
        }
        final SmartContext finalctx = ctx;

        sinkbuilder.setLocalhandler(this::process)
        .setCtx(ctx)
        .setType(Sockets.PULL);
        BlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);
        Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
        AtomicInteger count = new AtomicInteger();
        ThreadBuilder.get().setRunnable(() -> {
            try {
                while(finalctx.isRunning()) {
                    ev.put("message", count.incrementAndGet());
                    queue.offer(ev);
                    Thread.sleep(250);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).setDaemon(true).setName("EventSource").build(true);

        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setType(Sockets.PUSH.name());
        configure.accept(builder);

        try (ZMQSink sink = sinkbuilder.build() ; ZMQ r = builder.build()) {
            r.setInQueue(queue);
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            latch = new CountDownLatch(1);
            r.start();
            latch.await(1, TimeUnit.SECONDS);
        } finally {
            ctx.terminate();
        }
    }

    @Test(timeout=5000)
    public void bind() throws IOException, InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSink.Builder flowbuilder = new ZMQSink.Builder()
                        .setMethod(Method.BIND)
                        .setSource(rendezvous);
        dotest(null, (s) -> {
            s.setDestination(rendezvous);
            s.setMethod(Method.CONNECT.name());
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void connect() throws IOException, InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSink.Builder flowbuilder = new ZMQSink.Builder()
                        .setMethod(Method.CONNECT)
                        .setSource(rendezvous);
        dotest(null, (s) -> {
            s.setDestination(rendezvous);
            s.setMethod(Method.BIND.name());
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void curveClient() throws IOException, InterruptedException {
        Map<Object, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);

        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSink.Builder flowbuilder = new ZMQSink.Builder()
                        .setMethod(Method.CONNECT)
                        .setSource(rendezvous)
                        .setSecurity("Curve")
                        .setPrivateKey(serverKeys[1])
                        .setPublicKey(serverKeys[0]);

        dotest(ctx, (s) -> {
            s.setDestination(rendezvous);
            s.setMethod(Method.BIND.name());
            s.setSecurity("Curve");
            s.setServerKey("Curve "+ Base64.getEncoder().encodeToString(serverKeys[0]));
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void curveServer() throws IOException, InterruptedException {
        Map<Object, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream();
        Files.copy( Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.pub").toAbsolutePath(), pubkeyBuffer);
        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSink.Builder flowbuilder = new ZMQSink.Builder()
                        .setMethod(Method.CONNECT)
                        .setSource(rendezvous)
                        .setSecurity("Curve")
                        .setPrivateKey(serverKeys[1])
                        .setPublicKey(serverKeys[0])
                        .setServerKey(new String(pubkeyBuffer.toByteArray(), StandardCharsets.UTF_8));

        dotest(ctx, (s) -> {
            s.setDestination(rendezvous);
            s.setMethod(Method.BIND.name());
            s.setSecurity("Curve");
        }, flowbuilder);
    }

}
