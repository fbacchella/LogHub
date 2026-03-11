package loghub.netty.http2;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.netty.http.HttpRequestFailure;

public abstract class Http2Filter extends Http2Handler {

    protected Http2Filter() {
        super(false);
    }

    protected Http2Filter(boolean release) {
        super(release);
    }

    @Override
    protected final void subProcessing(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        filter(request, ctx);
        ctx.fireChannelRead(request);
    }

    protected abstract void filter(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure;

}
