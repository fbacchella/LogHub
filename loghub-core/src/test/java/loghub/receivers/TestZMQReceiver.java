package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore.PrivateKeyEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.ZMQFlow;
import loghub.configuration.Properties;
import loghub.decoders.Msgpack;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.types.MimeType;
import loghub.zmq.MsgHeaders;
import loghub.zmq.MsgHeaders.Header;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import zmq.io.mechanism.Mechanisms;

public class TestZMQReceiver {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.ContextRule", "loghub.ZMQFlow");
    }

    private static final Pattern ZMQ_SOCKETADDRESS_PATTERN = Pattern.compile("\\d+.\\d+.\\d+.\\d+:\\d+");
    @Rule(order = 1)
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Rule(order = 2)
    public final ZMQFactory tctxt = new ZMQFactory(testFolder, "secure");

    private Event dotest(Consumer<ZMQ.Builder> configure, Consumer<ZMQFlow.Builder> flowconfigure)
            throws InterruptedException {
        return dotest(configure, flowconfigure, null);
    }

    private Event dotest(Consumer<ZMQ.Builder> configure, Consumer<ZMQFlow.Builder> flowconfigure, byte[] header) throws
            InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSocketFactory ctx = tctxt.getFactory();
        PriorityBlockingQueue receiveQueue = new PriorityBlockingQueue();
        Properties p = new Properties(new HashMap<>(Map.of(
                "zmq.keystore", tctxt.getSecurityFolder().resolve("zmqtest.jks").toString(),
                "zmq.certsDirectory", tctxt.getSecurityFolder().resolve("certs").toString()
        )));

        try (ZMQFlow ignored1 = getFlow(ctx, header, rendezvous, flowconfigure); ZMQ ignored2 = getReceiver(rendezvous, configure, receiveQueue, p)) {
            Event e = receiveQueue.poll(2000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("No event received", e);
            ConnectionContext<String> connectionContext = e.getConnectionContext();
            Assert.assertTrue(ZMQ_SOCKETADDRESS_PATTERN.matcher(connectionContext.getLocalAddress()).matches());
            Assert.assertTrue(ZMQ_SOCKETADDRESS_PATTERN.matcher(connectionContext.getRemoteAddress()).matches());
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertTrue(e.get("message").toString().startsWith("message "));
            return e;
        } finally {
            p.getZMQSocketFactory().close();
        }
    }

    private ZMQFlow getFlow(ZMQSocketFactory ctx, byte[] header, String rendezvous, Consumer<ZMQFlow.Builder> flowconfigure) {
        BiPredicate<Socket, byte[]> sender;
        if (header == null) {
            sender = Socket::send;
        } else {
            sender = (s, b) -> {
                s.sendMore(header);
                return s.send(b);
            };
        }
        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                                                 .setDestination(rendezvous)
                                                 .setType(SocketType.PUSH)
                                                 .setZmqFactory(ctx)
                                                 .setSender(sender);
        AtomicInteger count = new AtomicInteger(0);
        flowbuilder.setSource(() -> String.format("message %s", count.incrementAndGet()).getBytes(StandardCharsets.UTF_8));
        flowconfigure.accept(flowbuilder);
        return flowbuilder.build();
    }

    private ZMQ getReceiver(String rendezvous, Consumer<ZMQ.Builder> configure, PriorityBlockingQueue receiveQueue, Properties p) {
        ZMQ.Builder builder = ZMQ.getBuilder();
        builder.setType(SocketType.PULL);
        builder.setDecoder(StringCodec.getBuilder().build());
        builder.setListen(rendezvous);
        builder.setEventsFactory(factory);
        configure.accept(builder);

        ZMQ receiver = builder.build();
        receiver.setOutQueue(receiveQueue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
        Assert.assertTrue(receiver.configure(p));
        Stats.registerReceiver(receiver);
        receiver.start();
        return receiver;
    }

    @Test(timeout = 5000)
    public void testConnect() throws InterruptedException {
        dotest(r -> {
            r.setMethod(Method.CONNECT);
            r.setType(SocketType.PULL);
        }, s -> s.setMethod(Method.BIND).setType(SocketType.PUSH).setMsPause(250));
    }

    @Test(timeout = 5000)
    public void testWithEmptyHeader() throws InterruptedException {
        dotest(r -> {
            r.setMethod(Method.CONNECT);
            r.setType(SocketType.PULL);
        }, s -> s.setMethod(Method.BIND).setType(SocketType.PUSH).setMsPause(250),
                new MsgHeaders().getContent());
    }

    @Test(timeout = 5000)
    public void testWithDecoderHeader() throws InterruptedException {
        dotest(r -> {
                    r.setMethod(Method.CONNECT);
                    r.setType(SocketType.PULL);
                    r.setDecoder(Msgpack.getBuilder().build());
                    r.setDecoders(Map.of("plain/text", StringCodec.getBuilder().build()));
                }, s -> s.setMethod(Method.BIND).setType(SocketType.PUSH).setMsPause(250),
                new MsgHeaders().addHeader(Header.MIME_TYPE, MimeType.of("plain/text")).getContent());
    }

    @Test(timeout = 5000)
    public void testWithInvalidHeader() throws InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQSocketFactory ctx = tctxt.getFactory();
        PriorityBlockingQueue receiveQueue = new PriorityBlockingQueue();
        Properties p = new Properties(new HashMap<>());
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        Supplier<byte[]> supplier = () -> {
            if (count.get() < 10) {
                return String.format("message %s", count.incrementAndGet()).getBytes(StandardCharsets.UTF_8);
            } else {
                latch.countDown();
                return null;
            }
        };
        Consumer<ZMQFlow.Builder> flowconfigure = s -> s.setMethod(Method.BIND).setType(SocketType.PUSH).setMsPause(5).setHwm(5).setSource(supplier);
        Consumer<ZMQ.Builder> configure = r -> {
            r.setMethod(Method.CONNECT);
            r.setType(SocketType.PULL);
            r.setDecoder(Msgpack.getBuilder().build());
            r.setDecoders(Map.of("plain/text", StringCodec.getBuilder().build()));
        };

        Stats.reset();
        try (ZMQFlow ignored1 = getFlow(ctx, new byte[]{0, 1}, rendezvous, flowconfigure); ZMQ ignored2 = getReceiver(rendezvous, configure, receiveQueue, p)) {
            latch.await();
        } finally {
            p.getZMQSocketFactory().close();
        }
        Assert.assertEquals("Failed to decode ZMQ message: Unable to decode header: Expected Map, but got Integer (00)", Stats.getReceiverError().stream().findAny().orElse(""));
    }

    @Test(timeout = 5000)
    public void testBind() throws InterruptedException {
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.PULL);
        }, s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(250));
    }

    @Test(timeout = 5000)
    public void testSub() throws InterruptedException {
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.SUB);
            r.setTopic("");
        }, s -> s.setMethod(Method.CONNECT).setType(SocketType.PUB).setMsPause(250));
    }

    @Test(timeout = 5000)
    public void testCurveExplicitClient() throws InterruptedException, IOException, GeneralSecurityException {
        Path keyPath = Paths.get("remote.jks");
        PrivateKeyEntry pve = tctxt.createKeyStore(keyPath, Map.of());
        String publicKey = ZMQHelper.makeServerIdentity(pve.getCertificate());
        dotest(r -> {
            r.setMethod(Method.BIND);
            r.setType(SocketType.PULL);
            r.setSecurity(Mechanisms.CURVE);
            r.setServerKey(publicKey);
        },
               s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(1000).setSecurity(Mechanisms.CURVE).setKeyEntry(pve)
        );
    }

    @Test(timeout = 5000)
    public void testCurveAnyClient()
            throws InterruptedException, IOException, GeneralSecurityException {
        Path keyPath = Paths.get("remote.jks");
        PrivateKeyEntry pve = tctxt.createKeyStore(keyPath, Map.of());
        dotest(r -> {
                r.setMethod(Method.BIND);
                r.setType(SocketType.PULL);
                r.setSecurity(Mechanisms.CURVE);
                r.setZapHandler(ZapDomainHandlerProvider.ALLOW);
            },
            s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(1000).setSecurity(Mechanisms.CURVE).setKeyEntry(pve).setServerKey(tctxt.loadServerPublicKey())
        );

    }

    @Test(timeout = 5000)
    public void testCertificateClient() throws InterruptedException, IOException, GeneralSecurityException {
        Path keyPath = Paths.get("remote.jks");
        PrivateKeyEntry pve = tctxt.createKeyStore(keyPath, Map.of("pipeline", "tester", "User-Id", "loghub"));
        Files.copy(tctxt.getRootFolder().resolve("remote.zpl"), tctxt.getSecurityFolder().resolve("certs").resolve("remote.zpl"));
        tctxt.getFactory();
        Event ev = dotest (r -> {
                    r.setMethod(Method.BIND);
                    r.setType(SocketType.PULL);
                    r.setSecurity(Mechanisms.CURVE);
                    r.setZapHandler(ZapDomainHandlerProvider.METADATA);
                },
                s -> s.setMethod(Method.CONNECT).setType(SocketType.PUSH).setMsPause(1000).setSecurity(Mechanisms.CURVE).setKeyEntry(pve).setServerKey(tctxt.loadServerPublicKey())
        );
        Assert.assertEquals("loghub", ev.getConnectionContext().getPrincipal().getName());
        Assert.assertEquals("tester", ev.getMeta("pipeline"));
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
                              , BeanInfo.build("decoders", Map.class)
                        );
    }

}
