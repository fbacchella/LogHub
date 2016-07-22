package loghub.netty;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;

public class LineReceiver extends TcpReceiver {

    private int maxLength = 256;
    private Charset charset= CharsetUtil.UTF_8;
    private String field = "message";

    public LineReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        splitter = new LineBasedFrameDecoder(maxLength);
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
