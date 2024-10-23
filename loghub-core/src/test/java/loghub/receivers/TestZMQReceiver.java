package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
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

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.ZMQFlow;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import zmq.io.mechanism.Mechanisms;

public class TestZMQReceiver {

    private static Logger logger;

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.ContextRule", "loghub.ZMQFlow");
    }

    private static final Pattern ZMQ_SOCKETADDRESS_PATTERN = Pattern.compile("\\d+.\\d+.\\d+.\\d+:\\d+");
    @Rule(order=1)
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Rule(order=2)
    public final ZMQFactory tctxt = new ZMQFactory(testFolder, "secure");

    private void dotest(Consumer<ZMQ.Builder> configure, Consumer<ZMQFlow.Builder> flowconfigure) throws IOException, InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();

        ZMQSocketFactory ctx = tctxt.getFactory();

        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setDestination(rendezvous)
                        .setType(SocketType.PUSH)
                        .setZmqFactory(ctx)
                        ;
        flowconfigure.accept(flowbuilder);

        AtomicInteger count = new AtomicInteger(0);
        flowbuilder.setSource(() -> String.format("message %s", count.incrementAndGet()).getBytes(StandardCharsets.UTF_8)); 
        PriorityBlockingQueue receiveQueue = new PriorityBlockingQueue();
        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setType(SocketType.PULL);
        builder.setDecoder(StringCodec.getBuilder().build());
        builder.setListen(rendezvous);
        configure.accept(builder);

        Properties p = new Properties(Collections.singletonMap("zmq.keystore", Paths.get(testFolder.newFolder().getAbsolutePath(), "zmqtest.jks").toString()));
        try (ZMQFlow flow = flowbuilder.build() ; ZMQ receiver = builder.build()) {
            receiver.setOutQueue(receiveQueue);
            receiver.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            Assert.assertTrue(receiver.configure(p));
            receiver.start();
            Event e = receiveQueue.poll(2000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("No event received", e);
            ConnectionContext<String> connectionContext = e.getConnectionContext();
            Assert.assertTrue(ZMQ_SOCKETADDRESS_PATTERN.matcher(connectionContext.getLocalAddress()).matches());
            Assert.assertTrue(ZMQ_SOCKETADDRESS_PATTERN.matcher(connectionContext.getRemoteAddress()).matches());
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertTrue(e.get("message").toString().startsWith("message "));
        } finally {
            p.getZMQSocketFactory().close();
        }
    }

    @Test(timeout=5000)
    public void testConnect() throws InterruptedException, IOException {
        dotest(r -> {
            r.setMethod(Method.CONNECT);
            r.setType(SocketType.PULL);
        }, s -> s.setMethod(Method.BIND).setType(SocketType.PUSH).setMsPause(250));
    }

    @Test(timeout=5000)
    public void testBind() throws InterruptedException, IOException {
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.PULL);
        }, s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(250));
    }

    @Test(timeout=5000)
    public void testSub() throws InterruptedException, IOException {
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.SUB);
            r.setTopic("");
        }, s -> s.setMethod(Method.CONNECT).setType(SocketType.PUB).setMsPause(250));
    }

    @Test(timeout=5000)
    public void testCurveServer() throws InterruptedException, IOException {
        Path keyPubpath = Paths.get(testFolder.getRoot().getPath(), "secure", "zmqtest.pub");
        String keyPub;
        try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
            Files.copy(keyPubpath, pubkeyBuffer);
            keyPub = pubkeyBuffer.toString(StandardCharsets.UTF_8);
        }
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.PULL);
            r.setSecurity(Mechanisms.CURVE);
            r.setServerKey(keyPub);
        },
               s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(1000).setSecurity(Mechanisms.CURVE).setKeyEntry(tctxt.getFactory().getKeyEntry()));
    }

    @Test(timeout=5000)
    public void testCurveClient() throws InterruptedException, IOException {
        Path keyPubpath = Paths.get(testFolder.getRoot().getPath(), "secure", "zmqtest.pub");
        String keyPub;
        try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
            Files.copy(keyPubpath, pubkeyBuffer);
            keyPub = pubkeyBuffer.toString(StandardCharsets.UTF_8);
        }

        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.PULL);
            r.setSecurity(Mechanisms.CURVE);
            r.setServerKey(keyPub);
        },
               s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(1000).setSecurity(Mechanisms.CURVE));

    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.ZMQ"
                              , BeanInfo.build("method", Method.class)
                              , BeanInfo.build("listen", String.class)
                              , BeanInfo.build("type", SocketType.class)
                              , BeanInfo.build("hwm", Integer.TYPE)
                              , BeanInfo.build("serverKey", String.class)
                              , BeanInfo.build("security", Mechanisms.class)
                              , BeanInfo.build("zapHandler", ZapDomainHandlerProvider.class)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
