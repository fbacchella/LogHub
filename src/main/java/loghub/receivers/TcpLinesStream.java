package loghub.receivers;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.AbstractTcpReceiver;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyReceiver;
import loghub.netty.servers.TcpServer;
import loghub.netty.transport.TRANSPORT;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(TcpLinesStream.Builder.class)
public class TcpLinesStream extends NettyReceiver<ByteBuf> implements ConsumerProvider {

    public static class Builder extends NettyReceiver.Builder<TcpLinesStream> {
        public Builder() {
            super();
            this.setTransport(TRANSPORT.TCP);
            // A ready to use TcpLinesStream: single line text message.
            StringCodec.Builder sbuilder = new StringCodec.Builder();
            this.setDecoder(sbuilder.build());
        }
        @Setter
        private int maxLength = 256;
        @Override
        public TcpLinesStream build() {
            return new TcpLinesStream(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final int maxLength;

    private TcpLinesStream(Builder builder) {
        super(builder);
        this.maxLength = builder.maxLength;
    }

    @Override
    public ChannelConsumer getConsumer() {
        return new BaseChannelConsumer<>(this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                super.addHandlers(pipe);
                pipe.addBefore("MessageDecoder", "Splitter", new LineBasedFrameDecoder(maxLength));
            }
        };
    }

    @Override
    public boolean configure(Properties properties) {
        builder.setThreadPrefix("LineReceiver");
        return super.configure(properties);
    }

    @Override
    public String getReceiverName() {
        return "LineReceiver/" + Helpers.ListenString(getHost()) + "/" + getPort();
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

}
