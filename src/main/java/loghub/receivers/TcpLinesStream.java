package loghub.receivers;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.AbstractTcpReceiver;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.servers.TcpServer;
import loghub.netty.servers.TcpServer.Builder;

public class TcpLinesStream extends AbstractTcpReceiver<TcpLinesStream, TcpServer, TcpServer.Builder> implements ConsumerProvider<TcpLinesStream, ServerBootstrap, ServerChannel> {

    private int maxLength = 256;
    private Charset charset= CharsetUtil.UTF_8;
    private String field = "message";

    public TcpLinesStream(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
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
    protected Builder getServerBuilder() {
        return TcpServer.getBuilder();
    }

    @Override
    public boolean configure(Properties properties, TcpServer.Builder builder) {
        StringCodec stringcodec = new StringCodec();
        stringcodec.setCharset(charset.toString());
        stringcodec.setField(field);
        decoder = stringcodec;
        return super.configure(properties, builder);
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * @return the charset
     */
    public String getCharset() {
        return charset.name();
    }

    /**
     * @param charset the charset to set
     */
    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

    @Override
    public String getReceiverName() {
        return "LineReceiver/" + getListenAddress();
    }

    /**
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String field) {
        this.field = field;
    }

}
