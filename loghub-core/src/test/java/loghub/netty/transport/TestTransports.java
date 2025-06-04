package loghub.netty.transport;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.DomainDatagramPacket;
import io.netty.util.CharsetUtil;
import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.ChannelConsumer;
import loghub.netty.transport.NettyTransport.Builder;

public class TestTransports {

    private static Logger logger;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    private abstract static class TestConsumer implements ChannelConsumer {
        Throwable exception = null;
        ByteBuf message;
        @Override
        public void exception(ChannelHandlerContext ctx, Throwable cause) {
            logger.atError().withThrowable(cause).log("Exception in context {}", ctx);
            exception = cause;
        }

        @Override
        public void logFatalException(Throwable ex) {
            logger.atError().withThrowable(ex).log("Fatal exception");
            exception = ex;
        }
    }

    private static class ServerConsumer extends TestConsumer {
        private final CountDownLatch received;

        private ServerConsumer(CountDownLatch received) {
            this.received = received;
        }

        @Override
        public void addHandlers(ChannelPipeline pipe) {
            pipe.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                    message = msg;
                    message.retain();
                    ctx.writeAndFlush(Unpooled.copiedBuffer("pong", CharsetUtil.UTF_8));
                }
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (msg instanceof DatagramPacket) {
                        DatagramPacket packet = (DatagramPacket) msg;
                        message = packet.content();
                        message.retain();
                        String response = "pong";
                        DatagramPacket reply = new DatagramPacket(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8),
                                packet.sender());
                        ctx.writeAndFlush(reply);
                    } else if (msg instanceof DomainDatagramPacket) {
                        DomainDatagramPacket packet = (DomainDatagramPacket) msg;
                        message = packet.content();
                        message.retain();
                        received.countDown();
                        // No response on unix datagram
                    } else {
                        super.channelRead(ctx, msg);
                    }
                }
            });
        }

    }

    private static class ClientConsumer extends TestConsumer {
        private final CountDownLatch received;

        private ClientConsumer(CountDownLatch received) {
            this.received = received;
        }

        @Override
        public void addHandlers(ChannelPipeline pipe) {
            pipe.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8));
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                    message = msg;
                    message.retain();
                    received.countDown();
                }
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (msg instanceof DatagramPacket) {
                        DatagramPacket packet = (DatagramPacket) msg;
                        message = packet.content();
                        message.retain();
                        received.countDown();
                    } else {
                        super.channelRead(ctx, msg);
                    }
                }
            });
        }
    }

    public NettyTransport<?, ?, ?, ?> getTransport(TRANSPORT transport, POLLER poller, String endpoint, int port, ChannelConsumer consumer) {
        Builder<? extends SocketAddress, Object, ?, ?> builder = transport.getBuilder();
        builder.setEndpoint(endpoint);
        builder.setPoller(poller);
        builder.setWorkerThreads(1);
        if (builder instanceof AbstractIpTransport.Builder && port >= 0) {
            AbstractIpTransport.Builder<?, ?, ?> ipbuilder = (AbstractIpTransport.Builder<?, ?, ?>) builder;
            ipbuilder.setPort(port);
        }
        builder.setConsumer(consumer);
        return builder.build();
    }

    private void runTest(TRANSPORT transport, POLLER poller, String host, int port)
            throws InterruptedException {
        CountDownLatch received = new CountDownLatch(1);
        TestConsumer clientConsumer;
        TestConsumer serverConsumer;
        NettyTransport<?, ?, ?, ?> server;
        NettyTransport<?, ?, ?, ?> client;
        clientConsumer = new ClientConsumer(received);
        serverConsumer = new ServerConsumer(received);
        server = getTransport(transport, poller, host, port, serverConsumer);
        client = getTransport(transport, poller, host, port, clientConsumer);

        server.bind();
        client.connect();

        received.await();
        client.close();
        server.close();
        Assert.assertNull(clientConsumer.exception);
        Assert.assertNull(serverConsumer.exception);
        Assert.assertEquals("ping", serverConsumer.message.toString(StandardCharsets.UTF_8));
        if (transport != TRANSPORT.UNIX_DGRAM) {
            Assert.assertEquals("pong", clientConsumer.message.toString(StandardCharsets.UTF_8));
            clientConsumer.message.release();
        }
        serverConsumer.message.release();
    }

    @Test(timeout = 1000)
    public void testLocal() throws InterruptedException {
        String address = TestTransports.class.getCanonicalName();
        runTest(TRANSPORT.LOCAL, POLLER.LOCAL, address, -1);
    }

    @Test(timeout = 1000)
    public void testTcpNio() throws InterruptedException {
        runTest(TRANSPORT.TCP, POLLER.NIO, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 1000)
    public void testTcpDefault() throws InterruptedException {
        runTest(TRANSPORT.TCP, POLLER.DEFAULTPOLLER, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 1000)
    public void testUdpNio() throws InterruptedException {
        runTest(TRANSPORT.UDP, POLLER.NIO, "localhost", 53065);
    }

    @Test(timeout = 1000)
    public void testUdpDefault() throws InterruptedException {
        runTest(TRANSPORT.UDP, POLLER.DEFAULTPOLLER, "localhost", Tools.tryGetPort());
    }

    @Test(timeout = 1000)
    public void testUnixStream() throws InterruptedException {
        runTest(TRANSPORT.UNIX_STREAM, POLLER.DEFAULTPOLLER, folder.getRoot().toPath().resolve("socketstream").toString(), -1);
    }

    @Test(timeout = 1000)
    public void testUnixDgram() throws InterruptedException {
        runTest(TRANSPORT.UNIX_DGRAM, POLLER.DEFAULTPOLLER, folder.getRoot().toPath().resolve("socketdgram").toString(), -1);
    }

}
