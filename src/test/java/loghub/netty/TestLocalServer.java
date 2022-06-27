package loghub.netty;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.transport.TRANSPORT;

public class TestLocalServer {

    @CloseOnError
    private static class TesterReceiver extends NettyReceiver<Object>
                                        implements ConsumerProvider {

        public static class Builder extends NettyReceiver.Builder<TesterReceiver> {
            public Builder() {
                setDecoder(StringCodec.getBuilder().build());
                setTransport(TRANSPORT.LOCAL);
                setHost(TestLocalServer.class.getCanonicalName());
            }
            @Override
            public TesterReceiver build() {
                return new TesterReceiver(this);
            }
        }
        public static Builder getBuilder() {
            return new Builder();
        }

        protected TesterReceiver(Builder builder) {
            super(builder);
            config.setThreadPrefix("ReceiverTest");

        }

        @Override
        public String getReceiverName() {
            return "ReceiverTest";
        }

        @Override
        public ByteBuf getContent(Object message) {
            logger.debug(message);
            return (ByteBuf) message;
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
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "io.netty");
    }

    @Test(timeout=2000)
    public void testSimple() throws InterruptedException {
        Properties empty = new Properties(Collections.emptyMap());
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        TesterReceiver.Builder builder = TesterReceiver.getBuilder();
        
        try (TesterReceiver r = builder.build()) {
            r.setPipeline(new Pipeline(Collections.emptyList(), "testone", null));
            r.setOutQueue(receiver);
            r.configure(empty);

            ChannelFuture[] sent = new ChannelFuture[1];
            CountDownLatch latch = new CountDownLatch(1);
            EventLoopGroup workerGroup = new DefaultEventLoopGroup();
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(LocalChannel.class);
            b.handler(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    sent[0] = ctx.writeAndFlush(Unpooled.copiedBuffer("Message\r\n", CharsetUtil.UTF_8));
                    latch.countDown();
                }
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                }

            });

            // Start the client.
            ChannelFuture f = b.connect(new LocalAddress(TestLocalServer.class.getCanonicalName())).sync();
            latch.await();
            sent[0].sync();
            f.channel().close();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();

            Event e = receiver.take();
            Assert.assertEquals("Message", e.get("message"));
        }
    }

}
