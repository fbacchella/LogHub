package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.Pipeline;

public class LineReceiver extends TcpReceiver<ByteBuf> {

    private int maxLength = 256;
    private Charset charset= CharsetUtil.UTF_8;

    public LineReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected ByteToMessageDecoder getNettyDecoder() {
        return new LineBasedFrameDecoder(maxLength);
    }

    @Override
    protected void populate(Event event, ChannelHandlerContext ctx, ByteBuf msg) {
        event.put("message", msg.toString(charset));
        SocketAddress addr = ctx.channel().remoteAddress();
        if(addr instanceof InetSocketAddress) {
            event.put("host", ((InetSocketAddress) addr).getAddress());
        }
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
    protected TcpServer<ByteBuf> getServer() {
        TcpServer<ByteBuf> server = new TcpServer<ByteBuf>();
        server.setIpAddr((InetSocketAddress) getAddress());
        return server;
    }

    @Override
    public String getReceiverName() {
        return null;
    }

}
