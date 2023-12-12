package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.decoders.StringCodec;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyReceiver;
import loghub.netty.transport.TRANSPORT;
import lombok.Setter;

@BuilderClass(TcpLinesStream.Builder.class)
@Blocking
public class TcpLinesStream extends NettyReceiver<TcpLinesStream, ByteBuf, TcpLinesStream.Builder> implements ConsumerProvider {

    public static class Builder extends NettyReceiver.Builder<TcpLinesStream, ByteBuf, TcpLinesStream.Builder> {
        public Builder() {
            super();
            setTransport(TRANSPORT.TCP);
            // A ready to use TcpLinesStream: single line text message.
            StringCodec.Builder sbuilder = new StringCodec.Builder();
            setDecoder(sbuilder.build());
        }
        @Setter
        private int maxLength = 256;
        @Setter
        private String separator = null;
        @Override
        public TcpLinesStream build() {
            return new TcpLinesStream(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Supplier<ByteToMessageDecoder> decoderSupplier;

    private TcpLinesStream(Builder builder) {
        super(builder);
        if (builder.separator == null) {
            decoderSupplier = () -> new LineBasedFrameDecoder(builder.maxLength);
        } else {
            ByteBuf buf = Unpooled.wrappedBuffer(builder.separator.getBytes(StandardCharsets.US_ASCII));
            decoderSupplier = () -> new DelimiterBasedFrameDecoder(builder.maxLength, buf);
        }
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "LineReceiver";
    }

    @Override
    public ChannelConsumer getConsumer() {
        return new BaseChannelConsumer<>(this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                super.addHandlers(pipe);
                pipe.addBefore("MessageDecoder", "Splitter", decoderSupplier.get());
            }
        };
    }

    @Override
    public String getReceiverName() {
        return "LineReceiver/" + Helpers.ListenString(getListen()) + "/" + getPort();
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

}
