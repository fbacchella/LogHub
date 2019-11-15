package loghub.zmq;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import loghub.LogUtils;
import loghub.Tools;
import loghub.zmq.ZMQHelper.Method;
import zmq.io.mechanism.curve.Curve;

public class CurveTest {

    private static Logger logger;

    @Rule
    public org.junit.rules.TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq");
    }

    @Test(timeout=5000)
    public void testSecureConnectOneWay() throws InterruptedException, ExecutionException, ZMQCheckedException {
        Map<String, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        Socket server = ctx.newSocket(Method.CONNECT, SocketType.PUSH, rendezvous, 100, 1000);
        Socket client = ctx.newSocket(Method.BIND, SocketType.PULL, rendezvous, 100, 1000);
        try {
            ctx.setCurveClient(client, serverKeys[0]);
            server.setCurveServer(true);
            server.setCurvePublicKey(serverKeys[0]);
            server.setCurveSecretKey(serverKeys[1]);

            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, server.getMechanism());
            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, client.getMechanism());

            server.send("Hello, World!");
            Assert.assertEquals("Hello, World!", client.recvStr());
        } finally {
            ctx.close(server);
            ctx.close(client);
            ctx.terminate();
        }
    }

    @Test(timeout=5000)
    public void testSecureConnectOtherWay() throws InterruptedException, ExecutionException, ZMQCheckedException {
        Map<String, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        Socket server = ctx.newSocket(Method.CONNECT, SocketType.PULL, rendezvous, 100, 1000);
        Socket client = ctx.newSocket(Method.BIND, SocketType.PUSH, rendezvous, 100, 1000);
        try {
            ctx.setCurveClient(client, serverKeys[0]);
            server.setCurveServer(true);
            server.setCurvePublicKey(serverKeys[0]);
            server.setCurveSecretKey(serverKeys[1]);

            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, server.getMechanism());
            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, client.getMechanism());

            client.send("Hello, World!");
            Assert.assertEquals("Hello, World!", server.recvStr());
        } finally {
            ctx.close(server);
            ctx.close(client);
            ctx.terminate();
        }
    }

    @Test(timeout=5000)
    public void testFailedSecureConnect() throws InterruptedException, ExecutionException, ZMQCheckedException {
        Map<String, Object> props = new HashMap<>();
        props.put("keystore", Paths.get(testFolder.getRoot().getAbsolutePath(), "zmqtest.jks").toAbsolutePath().toString());
        props.put("numSocket", 2);
        SmartContext ctx = SmartContext.build(props);

        Curve curve = new Curve();
        byte[][] serverKeys = curve.keypair();

        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        Socket server = ctx.newSocket(Method.CONNECT, SocketType.PUSH, rendezvous, 100, 1000);
        Socket client = ctx.newSocket(Method.BIND, SocketType.PULL, rendezvous, 100, 1000);
        try {
            // Putting the wrong key
            ctx.setCurveClient(client, serverKeys[1]);
            server.setCurveServer(true);
            server.setCurvePublicKey(serverKeys[0]);
            server.setCurveSecretKey(serverKeys[1]);

            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, server.getMechanism());
            Assert.assertEquals(ZMQ.Socket.Mechanism.CURVE, client.getMechanism());

            server.send("Hello, World!");
            Assert.assertNull(client.recvStr());
        } finally {
            ctx.close(server);
            ctx.close(client);
            ctx.terminate();
        }
    }

}
