package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore.PrivateKeyEntry;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.ZMQSink;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.ToJson;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.JmxService;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import zmq.io.mechanism.Mechanisms;

public class TestZMQSender {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @Rule(order = 1)
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Rule(order = 2)
    public final ZMQFactory tctxt = new ZMQFactory(testFolder, "secure");

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.ZMQSink", "loghub.senders.ZMQ", "loghub.ContextRule");
    }

    @After
    public void stopJmx() {
        JmxService.stop();
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
            throws InterruptedException, IOException, JMException {
        received.setLength(0);
        ZMQSocketFactory ctx = tctxt.getFactory();

        Properties p = new Properties(Collections.singletonMap("zmq.keystore", tctxt.getSecurityFolder().resolve("zmqtest.jks").toString()));

        int port = Tools.tryGetPort();
        String rendezvous = "tcp://localhost:" + port;

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
        builder.setType(SocketType.PUSH);
        builder.setDestination(rendezvous);
        configure.accept(builder);

        try (ZMQSink<String> ignored = sinkbuilder.build(); ZMQ sender = builder.build()) {
            p.jmxServiceConfiguration.registerSenders(List.of(sender));
            JmxService.start(p.jmxServiceConfiguration);
            Thread.sleep(100);
            sender.setInQueue(queue);
            Assert.assertTrue(sender.configure(p));
            latch = new CountDownLatch(4);
            sender.start();
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        } finally {
            injector.interrupt();
        }
        p.getZMQSocketFactory().close();
        String buf = received.toString();
        Assert.assertTrue(buf, Pattern.matches(pattern, buf));
        ObjectName on = ObjectName.getInstance("loghub", new Hashtable<>(
                Map.of(
                        "type", "Senders",
                        "servicename", "ZMQ/tcp/localhost/" + port
                )
        ));
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectInstance oi = mbs.getObjectInstance(on);
        Assert.assertEquals("loghub.metrics.SenderMBean$Implementation", oi.getClassName());
    }

    @Test(timeout = 5000)
    public void bind() throws InterruptedException, IOException, JMException {
         dotest(s ->  s.setMethod(Method.CONNECT),
                s -> s.setMethod(Method.BIND),
                "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout = 5000)
    public void connect() throws InterruptedException, IOException, JMException {
        dotest(s ->  s.setMethod(Method.BIND),
               s -> s.setMethod(Method.CONNECT),
               "(\\{\"message\":\\d+\\})+");
    }

    @Test(timeout  = 5000)
    public void batchConnect() throws InterruptedException, IOException, JMException {
        dotest(s -> {
            s.setMethod(Method.CONNECT);
            s.setBatchSize(2);
        }, s -> s.setMethod(Method.BIND), "(\\[\\{\"message\":\\d+\\},\\{\"message\":\\d+\\}\\])+");
    }

    @Test//(timeout = 5000)
    public void batchBind() throws InterruptedException, IOException, JMException {
        dotest(s -> {
            s.setMethod(Method.BIND);
            s.setBatchSize(2);
        }, s -> s.setMethod(Method.CONNECT), "(\\[\\{\"message\":\\d+\\},\\{\"message\":\\d+\\}\\])+");
    }

    @Test(timeout = 5000)
    public void curveClient() throws InterruptedException, GeneralSecurityException, IOException, JMException {
        Path keyPath = Paths.get("remote.jks");
        PrivateKeyEntry pve = tctxt.createKeyStore(keyPath, Map.of());
        String publicKey = ZMQHelper.makeServerIdentity(pve.getCertificate());
        dotest(
            s -> {
                s.setMethod(Method.BIND);
                s.setSecurity(Mechanisms.CURVE);
                s.setServerKey(publicKey);
            },
            s -> s.setMethod(Method.CONNECT)
                                .setKeyEntry(pve)
                                .setSecurity(Mechanisms.CURVE),
            "(\\{\"message\":\\d+\\})+"
            );
    }

    @Test(timeout = 5000)
    public void curveServer() throws InterruptedException, GeneralSecurityException, IOException, JMException {
        Path keyPath = Paths.get("remote.jks");
        PrivateKeyEntry pve = tctxt.createKeyStore(keyPath, Map.of());
        dotest(
            s -> {
                s.setMethod(Method.BIND);
                s.setSecurity(Mechanisms.CURVE);
            },
            s -> s.setMethod(Method.CONNECT)
                                .setKeyEntry(pve)
                                .setServerKey(tctxt.loadServerPublicKey())
                                .setSecurity(Mechanisms.CURVE),
            "(\\{\"message\":\\d+\\})+"
        );
    }

    @Test(timeout = 2000)
    public void testEncodeError() throws InterruptedException, EncodeException {
        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setEncoder(ToJson.getBuilder().build());
        builder.setType(SocketType.PUSH);
        builder.setDestination("tcp://localhost:" + Tools.tryGetPort());
        SenderTools.send(builder);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.ZMQ"
                              , BeanInfo.build("method", Method.class)
                              , BeanInfo.build("destination", String.class)
                              , BeanInfo.build("type", SocketType.class)
                              , BeanInfo.build("hwm", Integer.TYPE)
                              , BeanInfo.build("serverKey", String.class)
                              , BeanInfo.build("security", Mechanisms.class)
                              , BeanInfo.build("zapHandler", ZapDomainHandlerProvider.class)
                              , BeanInfo.build("withHeader", boolean.class)
                        );
    }

}
