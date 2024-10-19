package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyReceiver;
import loghub.netty.transport.TRANSPORT;
import lombok.Setter;

@Blocking
public abstract class AbstractNettyStream<R extends AbstractNettyStream<R, B>, B extends AbstractNettyStream.Builder<R, B>> extends NettyReceiver<R, ByteBuf, B>
        implements ConsumerProvider {

    @Setter
    public abstract static class Builder<R extends AbstractNettyStream<R, B>, B extends AbstractNettyStream.Builder<R, B>> extends NettyReceiver.Builder<R, ByteBuf, B> {
        protected Builder() {
            super();
            setTransport(TRANSPORT.TCP);
        }
        protected int maxLength = 256;
        protected String separator = null;
    }

    protected final Supplier<ByteToMessageDecoder> splitterSupplier;

    protected AbstractNettyStream(B builder) {
        super(builder);
        if (builder.separator == null) {
            splitterSupplier = () -> new LineBasedFrameDecoder(builder.maxLength);
        } else {
            ByteBuf buf = Unpooled.wrappedBuffer(builder.separator.getBytes(StandardCharsets.US_ASCII));
            splitterSupplier = () -> new DelimiterBasedFrameDecoder(builder.maxLength, buf);
        }
    }


    @Override
    public ChannelConsumer getConsumer() {
        return new BaseChannelConsumer<>((R) this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                super.addHandlers(pipe);
                pipe.addAfter("ContentExtractor", "Splitter", splitterSupplier.get());
                updateHandler(pipe);
            }
        };
    }

    protected void updateHandler(ChannelPipeline pipe) {

    }
    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

}
