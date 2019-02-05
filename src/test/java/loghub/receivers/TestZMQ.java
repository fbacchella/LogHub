package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Socket.Mechanism;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ContextRule;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper.Method;
import zmq.io.mechanism.curve.Curve;
import zmq.socket.Sockets;

public class TestZMQ {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();
    
    @Rule
    public org.junit.rules.TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.ContextRule");
    }

    private void dotest(Consumer<ZMQ> configure, Socket sender) throws InterruptedException  {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        try (ZMQ r = new ZMQ()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            configure.accept(r);
            r.setDecoder(StringCodec.getBuilder().build());
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            logger.debug("before");
            r.start();
            Thread.sleep(30);
            logger.debug("after");
            Assert.assertTrue(sender.send("message 1"));
            Event e = receiver.take();
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertEquals("Missing message", "message 1", e.get("message"));
            r.stopReceiving();
            r.close();
        }
    }

    @Test(timeout=500)
    public void testone() throws InterruptedException {
        try (Socket sender = tctxt.ctx.newSocket(Method.BIND, Sockets.PUB, "inproc://listener1")) {
            dotest(r -> {
                r.setListen("inproc://listener1");
                r.setMethod("CONNECT");
                r.setType("SUB");
            }, sender);
        };
    }

    @Test(timeout=5000)
    public void testtwo() throws InterruptedException {
        try (Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Sockets.PUB, "inproc://listener2")) {
            dotest(r -> {
                r.setListen("inproc://listener2");
                r.setMethod("BIND");
                r.setType("SUB");
            }, sender);
        };
    }

    @Test(timeout=5000)
    public void testCurve() throws InterruptedException {
        tctxt.ctx.terminate();
        
        Map<Object, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        try (Socket sender = ctx.newSocket(Method.CONNECT, Sockets.PUSH, rendezvous)) {
            sender.setCurveServer(true);
            sender.setCurvePublicKey(serverKeys[0]);
            sender.setCurveSecretKey(serverKeys[1]);
            dotest(r -> {
                r.setListen(rendezvous);
                r.setMethod("BIND");
                r.setType("PULL");
                r.setSecurity("Curve");
                r.setServerKey("Curve "+ Base64.getEncoder().encodeToString(serverKeys[0]));
            }, sender);
            Assert.assertEquals(Mechanism.CURVE, sender.getMechanism());
            ctx.close(sender);
        } finally {
            ctx.terminate();
        };
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
