package loghub.receivers;

import java.nio.charset.Charset;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.BuilderClass;
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
        @Setter
        private int maxLength = 256;
        @Setter
        private String charset= CharsetUtil.UTF_8.name();
        @Setter
        private String field = "message";
        @Override
        public TcpLinesStream build() {
            StringCodec.Builder sbuilder = new StringCodec.Builder();
            sbuilder.setCharset(charset.toString());
            sbuilder.setField(field);
            this.setDecoder(sbuilder.build());
            return new TcpLinesStream(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final int maxLength;

    private final Charset charset;
    @Getter
    private final String field;

    protected TcpLinesStream(Builder builder) {
        super(builder);
        this.maxLength = builder.maxLength;
        this.charset = Charset.forName(builder.charset);
        this.field = builder.field;
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

    /**
     * @return the charset
     */
    public String getCharset() {
        return charset.name();
    }

    @Override
    public String getReceiverName() {
        return "LineReceiver/" + getListenAddress();
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

}
