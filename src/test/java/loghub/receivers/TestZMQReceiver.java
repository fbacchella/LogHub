package loghub.receivers;

import java.beans.IntrospectionException;
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
import org.zeromq.SocketType;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.ZMQFlow;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper.Method;
import zmq.io.mechanism.curve.Curve;

public class TestZMQReceiver {

    private static Logger logger;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.ContextRule", "loghub.ZMQFlow");
    }

    private void dotest(SmartContext ctx, Consumer<ZMQ> configure, ZMQFlow.Builder flowbuilder) throws IOException, InterruptedException {
        if (ctx == null) {
            ctx = SmartContext.getContext();
        }
        AtomicInteger count = new AtomicInteger(0);
        flowbuilder.setSource(() -> String.format("message %s", count.incrementAndGet()).getBytes(StandardCharsets.UTF_8)); 
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(100);
        try (ZMQFlow flow = flowbuilder.build() ; ZMQ r = new ZMQ()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            configure.accept(r);
            r.setDecoder(StringCodec.getBuilder().build());
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            r.start();
            Event e = receiver.poll(2000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("No event received", e);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertTrue(e.get("message").toString().startsWith("message "));
        } finally {
            ctx.terminate();
        }
    }

    @Test(timeout=5000)
    public void testConnect() throws InterruptedException, IOException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.BIND)
                        .setDestination(rendezvous)
                        .setType(SocketType.PUSH)
                        .setMsPause(250);
        dotest(null, r -> {
            r.setListen(rendezvous);
            r.setMethod("CONNECT");
            r.setType("PULL");
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void testBind() throws InterruptedException, IOException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.CONNECT)
                        .setDestination(rendezvous)
                        .setType(SocketType.PUSH)
                        .setMsPause(1000);
        dotest(null, r -> {
            r.setListen(rendezvous);
            r.setMethod("BIND");
            r.setType("PULL");
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void testSub() throws InterruptedException, IOException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.CONNECT)
                        .setDestination(rendezvous)
                        .setType(SocketType.PUB)
                        .setMsPause(1000);
        dotest(null, r -> {
            r.setListen(rendezvous);
            r.setMethod("BIND");
            r.setType("SUB");
            r.setTopic("");
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void testCurveServer() throws InterruptedException, IOException {
        Map<Object, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();

        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.CONNECT)
                        .setDestination(rendezvous)
                        .setType(SocketType.PUSH)
                        .setMsPause(1000)
                        .setSecurity("Curve")
                        .setPrivateKey(serverKeys[1])
                        .setPublicKey(serverKeys[0]);
        dotest(ctx, r -> {
            r.setListen(rendezvous);
            r.setMethod("BIND");
            r.setType("PULL");
            r.setSecurity("Curve");
            r.setServerKey("Curve "+ Base64.getEncoder().encodeToString(serverKeys[0]));
        }, flowbuilder);
    }

    @Test(timeout=5000)
    public void testCurveClient() throws InterruptedException, IOException {
        Map<Object, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream();
        Files.copy( Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.pub").toAbsolutePath(), pubkeyBuffer);
        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();

        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.CONNECT)
                        .setDestination(rendezvous)
                        .setType(SocketType.PUSH)
                        .setMsPause(1000)
                        .setSecurity("Curve")
                        .setPrivateKey(serverKeys[1])
                        .setPublicKey(serverKeys[0])
                        .setServerKey(new String(pubkeyBuffer.toByteArray(), StandardCharsets.UTF_8));
        dotest(ctx, r -> {
            r.setListen(rendezvous);
            r.setMethod("BIND");
            r.setType("PULL");
            r.setSecurity("Curve");
        }, flowbuilder);
    }

    @Test
    public void testBeans() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.receivers.ZMQ"
                              ,BeanInfo.build("method", String.class)
                              ,BeanInfo.build("listen", String.class)
                              ,BeanInfo.build("type", String.class)
                              ,BeanInfo.build("hwm", Integer.TYPE)
                              ,BeanInfo.build("serverKey", String.class)
                              ,BeanInfo.build("security", String.class)
                        );
    }

}
