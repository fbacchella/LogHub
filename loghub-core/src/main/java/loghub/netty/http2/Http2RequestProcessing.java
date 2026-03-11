package loghub.netty.http2;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.netty.http.HttpRequestFailure;

public abstract class Http2RequestProcessing extends Http2Handler {

    protected Http2RequestProcessing() {
        super(true);
    }

    protected Http2RequestProcessing(boolean release) {
        super(release);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void subProcessing(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        processRequest(request, ctx);
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
    }

    protected abstract void processRequest(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure;

}
