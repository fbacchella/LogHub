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
import io.netty.channel.local.LocalAddress;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TransportConfig;

public class TestLocalServer {

    @CloseOnError
    private static class TesterReceiver extends NettyReceiver<TesterReceiver, Object>
                                        implements ConsumerProvider {

        public static class Builder extends NettyReceiver.Builder<TesterReceiver,Object> {
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
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
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

            NettyTransport<LocalAddress, ByteBuf> client = TRANSPORT.LOCAL.getInstance(POLLER.DEFAULTPOLLER);
            TransportConfig config = new TransportConfig();
            config.setEndpoint(TestLocalServer.class.getCanonicalName());
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
            ChannelFuture cf = client.connect(config);
            cf.addListener(gfl -> cf.channel().flush()).addListener(gfl -> cf.channel().close());
            // Wait until the connection is closed.
            cf.channel().closeFuture().sync();

            Event e = receiver.take();
            Assert.assertEquals("Message", e.get("message"));
        }
    }

}
