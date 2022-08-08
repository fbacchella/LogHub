package loghub.netty;

import java.io.IOException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.DomainDatagramPacket;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;

public class TestSimpleReceiver {

    @CloseOnError
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
            //builder.setThreadPrefix("ReceiverTest");
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
                return ((DatagramPacket)message).content();
            } else if (message instanceof DomainDatagramPacket) {
                return ((DomainDatagramPacket)message).content();
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

    @BeforeClass
    static public void configure() throws IOException {
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
            if (config instanceof AbstractIpTransport.Builder) {
                AbstractIpTransport.Builder<?, ?, ?> ipbuilder = (AbstractIpTransport.Builder<?, ?, ?>) config;
                ipbuilder.setPort(port);
            }
            config.setConsumer(new ChannelConsumer() {
                @Override
                public void addHandlers(ChannelPipeline pipe) {
                    pipe.channel().write(Unpooled.copiedBuffer("Message\r\n", CharsetUtil.UTF_8));
                }

                @Override
                public void exception(ChannelHandlerContext ctx, Throwable cause) {
                    cause.printStackTrace();
                }

                @Override
                public void logFatalException(Throwable ex) {
                    ex.printStackTrace();
                }
            });
            NettyTransport<?, ?, ?, ?> client = config.build();
            ChannelFuture cf = client.connect();
            cf.addListener(gfl -> cf.channel().flush()).addListener(gfl -> cf.channel().close());
            // Wait until the connection is closed.
            cf.channel().closeFuture().sync();

            Event e = receiver.take();
            Assert.assertEquals("Message", e.get("message"));
        }
    }

    @Test(timeout = 5000)
    public void testLocal() throws InterruptedException {
        runTest(TRANSPORT.LOCAL, POLLER.DEFAULTPOLLER, TestSimpleReceiver.class.getCanonicalName(), -1);
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
        runTest(TRANSPORT.UNIX_STREAM, POLLER.DEFAULTPOLLER, "socketstream", Tools.tryGetPort());
    }

    @Test(timeout = 5000)
    public void testUnixDgram() throws InterruptedException {
        runTest(TRANSPORT.UNIX_DGRAM, POLLER.DEFAULTPOLLER, "socketdgram", Tools.tryGetPort());
    }

}
