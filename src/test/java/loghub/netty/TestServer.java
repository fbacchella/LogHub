package loghub.netty;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.servers.AbstractNettyServer;
import loghub.netty.servers.ServerFactory;

public class TestServer {

    private static class LocalChannelConnectionContext extends ConnectionContext<LocalAddress> {
        private final LocalAddress local;
        private final LocalAddress remote;
        private LocalChannelConnectionContext(LocalChannel channel) {
            this.local = channel.localAddress();
            this.remote = channel.remoteAddress();
        }
        @Override
        public LocalAddress getLocalAddress() {
            return local;
        }
        @Override
        public LocalAddress getRemoteAddress() {
            return remote;
        }
    };

    private static class TesterFactory extends ServerFactory<LocalChannel, LocalAddress> {
        private static final ChannelFactory<ServerChannel> channelfactory = new ChannelFactory<ServerChannel>() {
            @Override 
            public LocalServerChannel newChannel() {
                return new LocalServerChannel();
            }
        };

        @Override
        public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new DefaultEventLoopGroup(threads, threadFactory);
        }

        @Override
        public ChannelFactory<ServerChannel> getInstance() {
            return channelfactory;
        }
    };

    private static class TesterServer extends AbstractNettyServer<TesterFactory, ServerBootstrap, ServerChannel, LocalServerChannel, LocalAddress, TesterServer, TesterServer.Builder> {

        public static class Builder extends  AbstractNettyServer.Builder<TesterServer, TesterServer.Builder> {
            public TesterServer build() {
                return new TesterServer(this);
            }
        }

        Channel cf;

        public TesterServer(Builder builder) {
            super(builder);
        }

        @Override
        protected TesterFactory getNewFactory() {
            return new TesterFactory();
        }

        @Override
        protected boolean makeChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> bootstrap, LocalAddress address) {
            // Bind and start to accept incoming connections.
            try {
                cf = bootstrap.bind(address).await().channel();
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public void waitClose() throws InterruptedException {
            cf.closeFuture().sync();
        }

        @Override
        protected LocalAddress setAddress(Builder builder) {
            return new LocalAddress(TestServer.class.getCanonicalName());
        }
    }

    @CloseOnError
    private static class TesterReceiver extends NettyReceiver<TesterReceiver, TesterServer, TesterServer.Builder, TesterFactory, ServerBootstrap, ServerChannel, LocalServerChannel, LocalChannel, LocalAddress, Object>
                                        implements ConsumerProvider<TesterReceiver, ServerBootstrap, ServerChannel>{

        public TesterReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
            super(outQueue, pipeline);
            decoder = new StringCodec();
        }

        @Override
        public String getReceiverName() {
            return "ReceiverTest";
        }

        @Override
        public ConnectionContext<LocalAddress> getNewConnectionContext(ChannelHandlerContext ctx, Object message) {
            return new LocalChannelConnectionContext((LocalChannel) ctx.channel());
        }

        @Override
        protected TesterServer.Builder getServerBuilder() {
            return new TesterServer.Builder();
        }

        @Override
        public ByteBuf getContent(Object message) {
            logger.debug(message);
            return (ByteBuf) message;
        }

        @Override
        public ChannelConsumer<ServerBootstrap, ServerChannel> getConsumer() {
            return new BaseChannelConsumer<TesterReceiver, ServerBootstrap, ServerChannel, Object>(this) {
                @Override
                public void addHandlers(ChannelPipeline pipe) {
                    super.addHandlers(pipe);
                    pipe.addBefore("MessageDecoder", "Splitter", new LineBasedFrameDecoder(256));
                }
            };
        }

    }

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "io.netty");
    }

    @Test(timeout=2000)
    public void testSimple() throws InterruptedException {
        Properties empty = new Properties(Collections.emptyMap());
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        try(TesterReceiver r = new TesterReceiver(receiver, new Pipeline(Collections.emptyList(), "testone", null))) {
            r.configure(empty);

            ChannelFuture[] sent = new ChannelFuture[1];

            EventLoopGroup workerGroup = new DefaultEventLoopGroup();
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(LocalChannel.class);
            b.handler(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    sent[0] = ctx.writeAndFlush(Unpooled.copiedBuffer("Message\r\n", CharsetUtil.UTF_8));
                }
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                }

            });

            // Start the client.
            ChannelFuture f = b.connect(new LocalAddress(TestServer.class.getCanonicalName())).sync();
            Thread.sleep(100);
            sent[0].sync();
            f.channel().close();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();

            Event e = receiver.poll();
            Assert.assertEquals("Message", e.get("message"));
        }
    }
}
