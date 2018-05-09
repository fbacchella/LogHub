package loghub.receivers;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.netty.GenericTcp;

public class TcpLinesStream extends GenericTcp {

    private int maxLength = 256;
    private Charset charset= CharsetUtil.UTF_8;
    private String field = "message";

    public TcpLinesStream(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public void addHandlers(ChannelPipeline pipe) {
        pipe.addFirst("Splitter", new LineBasedFrameDecoder(maxLength));
        super.addHandlers(pipe);
    }

    @Override
    public boolean configure(Properties properties) {
        StringCodec stringcodec = new StringCodec();
        stringcodec.setCharset(charset.toString());
        stringcodec.setField(field);
        decoder = stringcodec;
        return super.configure(properties);
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
