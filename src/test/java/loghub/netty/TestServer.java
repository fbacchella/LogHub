package loghub.netty;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestServer {
    private static class TesterFactory extends ServerFactory<LocalServerChannel, LocalChannel> {
        private static final ChannelFactory<LocalServerChannel> channelfactory = new ChannelFactory<LocalServerChannel>() {
            @Override 
            public LocalServerChannel newChannel() {
                return new LocalServerChannel();
            }
        };

        @Override
        public EventLoopGroup getEventLoopGroup() {
            return new DefaultEventLoopGroup();
        }

        @Override
        public ChannelFactory<LocalServerChannel> getInstance() {
            return channelfactory;
        }
    };

    private static class TesterServer extends AbstractNettyServer<TesterFactory, ServerBootstrap, ServerChannel, LocalServerChannel, LocalChannel, ByteBuf> {

        @Override
        protected TesterFactory getNewFactory(Properties properties) {
            return new TesterFactory();
        }
        @Override
        protected SocketAddress getAddress() {
            return new LocalAddress(TestServer.class.getCanonicalName());
        }
        @Override
        public ChannelFuture configure(Properties properties, HandlersSource<LocalServerChannel, LocalChannel> source) {
            logger.debug("server configured");
            return super.configure(properties, source);
        }
        

    }

    private static class TesterReceiver extends NettyReceiver<TesterServer, TesterFactory, ServerBootstrap, ServerChannel, LocalServerChannel, LocalChannel, ByteBuf> {

        public TesterReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
            super(outQueue, pipeline);
        }

        @Override
        protected ByteToMessageDecoder getNettyDecoder() {
            return new LineBasedFrameDecoder(256);
        }

        @Override
        protected void populate(Event event, ChannelHandlerContext ctx, ByteBuf msg) {
            logger.debug(msg);
            event.put("message", msg.toString(CharsetUtil.UTF_8));
            SocketAddress addr = ctx.channel().remoteAddress();
            if(addr instanceof LocalAddress) {
                event.put("host", ((LocalAddress) addr).id());
            }
        }

        @Override
        protected SocketAddress getAddress() {
            return new LocalAddress(TestServer.class.getCanonicalName());
        }

        @Override
        protected TesterServer getServer() {
            return new TesterServer();
        }

        @Override
        public String getReceiverName() {
            return "ReceiverTest";
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
        TesterReceiver r = new TesterReceiver(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        r.configure(empty);
        r.start();

        final ChannelFuture[] sent = new ChannelFuture[1];

        EventLoopGroup workerGroup = new DefaultEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(LocalChannel.class);
        b.handler(new SimpleChannelInboundHandler<ByteBuf>() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                logger.debug("connected");
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
        r.interrupt();

        // Wait until the connection is closed.
        f.channel().closeFuture().sync();

        Event e = receiver.poll();
        Assert.assertEquals("Message", e.get("message"));

    }
}
