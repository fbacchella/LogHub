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
import loghub.netty.servers.TcpServer;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(TcpLinesStream.Builder.class)
public class TcpLinesStream extends AbstractTcpReceiver<TcpLinesStream, TcpServer, TcpServer.Builder, ByteBuf> implements ConsumerProvider<TcpLinesStream, ServerBootstrap, ServerChannel> {

    public static class Builder extends AbstractTcpReceiver.Builder<TcpLinesStream> {
        public Builder() {
            super();
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
    };
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
    public ChannelConsumer<ServerBootstrap, ServerChannel> getConsumer() {
        return new BaseChannelConsumer<TcpLinesStream, ServerBootstrap, ServerChannel, ByteBuf>(this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                super.addHandlers(pipe);
                pipe.addBefore("MessageDecoder", "Splitter", new LineBasedFrameDecoder(maxLength));
            }
        };
    }

    @Override
    protected TcpServer.Builder getServerBuilder() {
        return TcpServer.getBuilder();
    }

    @Override
    public boolean configure(Properties properties, TcpServer.Builder builder) {
        builder.setThreadPrefix("LineReceiver");
        return super.configure(properties, builder);
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
