package loghub.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import loghub.configuration.Properties;

public abstract class HttpRequestProcessing extends HttpHandler {

    protected HttpRequestProcessing() {
        super(true);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public final boolean acceptInboundMessage(Object msg) throws Exception {
        return super.acceptInboundMessage(msg) && acceptRequest((HttpRequest)msg);
    }

    @Override
    protected void subProcessing(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        if (!processRequest(request, ctx)) {
            // If keep-alive is off, close the connection once the content is fully written.
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
        }
        Properties.metrics.meter("WebServer.status.200").mark();
    }

    public abstract boolean acceptRequest(HttpRequest request);

    protected abstract boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

}
