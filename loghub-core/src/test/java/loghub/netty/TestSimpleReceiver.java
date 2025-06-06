package loghub.netty;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.DomainDatagramPacket;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.BuilderClass;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.events.Event;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;

public class TestSimpleReceiver {

    @CloseOnError
    @BuilderClass(TesterReceiver.Builder.class)
    private static class TesterReceiver extends NettyReceiver<TestSimpleReceiver.TesterReceiver, Object, TestSimpleReceiver.TesterReceiver.Builder>
            implements ConsumerProvider {

        public static class Builder extends NettyReceiver.Builder<TestSimpleReceiver.TesterReceiver, Object, TestSimpleReceiver.TesterReceiver.Builder> {
            public Builder() {
                setDecoder(StringCodec.getBuilder().build());
            }
            @Override
            public TestSimpleReceiver.TesterReceiver build() {
                return new TestSimpleReceiver.TesterReceiver(this);
            }
        }
        public static TestSimpleReceiver.TesterReceiver.Builder getBuilder() {
            return new TestSimpleReceiver.TesterReceiver.Builder();
        }

        protected TesterReceiver(TestSimpleReceiver.TesterReceiver.Builder builder) {
            super(builder);
        }

        @Override
        protected String getThreadPrefix(Builder builder) {
            return "ReceiverTest";
        }

        @Override
        public String getReceiverName() {
            return "ReceiverTest";
        }

        @Override
        public ByteBuf getContent(Object message) {
            if (message instanceof ByteBuf) {
                return (ByteBuf) message;
            } else if (message instanceof DatagramPacket) {
                return ((DatagramPacket) message).content();
            } else if (message instanceof DomainDatagramPacket) {
                return ((DomainDatagramPacket) message).content();
            }
            throw new IllegalArgumentException(message.getClass().getCanonicalName());
        }

        @Override
        public ChannelConsumer getConsumer() {
            return new BaseChannelConsumer<>(this) {
                @Override
                public void addHandlers(ChannelPipeline pipe) {
                    super.addHandlers(pipe);
                    pipe.addBefore("MessageDecoder", "Splitter", new LineBasedFrameDecoder(256));
                }
            };
        }

    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    private void runTest(TRANSPORT transport, POLLER poller, String host, int port)
            throws InterruptedException {
        Properties empty = new Properties(Collections.emptyMap());
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        TestSimpleReceiver.TesterReceiver.Builder builder = TestSimpleReceiver.TesterReceiver.getBuilder();
        builder.setTransport(transport);
        builder.setPoller(poller);
        builder.setHost(host);
        builder.setPort(port);

        try (TestSimpleReceiver.TesterReceiver r = builder.build()) {
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            r.setOutQueue(receiver);
            r.configure(empty);

            NettyTransport.Builder<?, ?, ?, ?> config = transport.getBuilder();
            config.setEndpoint(host);
            config.setPoller(poller);
            if (config instanceof AbstractIpTransport.Builder) {
                AbstractIpTransport.Builder<?, ?, ?> ipbuilder = (AbstractIpTransport.Builder<?, ?, ?>) config;
                ipbuilder.setPort(port);
            }
            AtomicReference<Throwable> exceptionReference = new AtomicReference<>();
            config.setConsumer(new ChannelConsumer() {
                @Override
                public void addHandlers(ChannelPipeline pipe) {
                    pipe.channel().write(Unpooled.copiedBuffer("Message\r\n", CharsetUtil.UTF_8));
                }

                @Override
                public void exception(ChannelHandlerContext ctx, Throwable cause) {
                    exceptionReference.set(cause);
                }

                @Override
                public void logFatalException(Throwable ex) {
                    exceptionReference.set(ex);
                }
            });
            NettyTransport<?, ?, ?, ?> client = config.build();
            ChannelFuture cf = client.connect();
            cf.addListener(gfl -> cf.channel().flush()).addListener(gfl -> cf.channel().close());
            // Wait until the connection is closed.
            cf.channel().closeFuture().sync();

            Event e = receiver.take();
            Assert.assertEquals("Message", e.get("message"));
            Assert.assertNull(exceptionReference.get());
        }
    }

    @Test(timeout = 5000)
    public void testLocal() throws InterruptedException {
        runTest(TRANSPORT.LOCAL, POLLER.LOCAL, TestSimpleReceiver.class.getCanonicalName(), -1);
    }

    @Test(timeout = 5000)
    public void testTcpNio() throws InterruptedException {
        runTest(TRANSPORT.TCP, POLLER.NIO, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testTcpDefault() throws InterruptedException {
        runTest(TRANSPORT.TCP, POLLER.DEFAULTPOLLER, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testUdpNio() throws InterruptedException {
        runTest(TRANSPORT.UDP, POLLER.NIO, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testUdpDefault() throws InterruptedException {
        runTest(TRANSPORT.UDP, POLLER.DEFAULTPOLLER, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testUnixStream() throws InterruptedException {
        runTest(TRANSPORT.UNIX_STREAM, POLLER.DEFAULTPOLLER, folder.getRoot().toPath().resolve("socketstream").toString(), Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testUnixDgram() throws InterruptedException {
        runTest(TRANSPORT.UNIX_DGRAM, POLLER.DEFAULTPOLLER, folder.getRoot().toPath().resolve("socketdgram").toString(), Tools.tryGetPort());
    }

    private void parseReceiverConfig(String config) throws IOException {
        Properties conf = Configuration.parse(new StringReader(config));
        try (NettyReceiver<?, ?, ?> r = (NettyReceiver<?, ?, ?>) conf.receivers.stream().findAny().orElseThrow(() -> new IllegalStateException("No received defined"))) {
            Assert.assertEquals(POLLER.NIO, r.transport.getPoller());
        }
    }
    @Test
    public void testParsing() throws IOException {
        parseReceiverConfig("input {\n" + "    loghub.netty.TestSimpleReceiver$TesterReceiver { transport: \"TCP\", poller: \"NIO\"            }\n" + "} | $main pipeline[main]{}\n");
    }

    @Test
    public void testPollerProperty() throws IOException {
        parseReceiverConfig("poller: \"NIO\" input {\n" + "    loghub.netty.TestSimpleReceiver$TesterReceiver { transport: \"TCP\",             }\n" + "} | $main pipeline[main]{}\n");
    }

    @Test
    public void testWrongPollerProperty() {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> parseReceiverConfig("poller: \"NONE\" input {\n" + "    loghub.netty.TestSimpleReceiver$TesterReceiver { transport: \"TCP\",             }\n" + "} | $main pipeline[main]{}\n"));
        Assert.assertEquals("Unhandled poller: \"NONE\"", ex.getMessage());
    }

}
