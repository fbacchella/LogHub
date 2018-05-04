package loghub.netty.http;

import java.util.Date;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

public abstract class HttpFilter extends HttpHandler {

    HttpFilter() {
        super(false);
    }

    @Override
    public final boolean acceptInboundMessage(Object msg) throws Exception {
        return super.acceptInboundMessage(msg) && acceptRequest((HttpRequest)msg);
    }

    public boolean acceptRequest(HttpRequest request) {
        return true;
    }

    @Override
    protected final void subProcessing(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        filter(request, ctx);
        ctx.fireChannelRead(request);
    }

    protected abstract void filter(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        return null;
    }

    @Override
    protected Date getContentDate(HttpRequest request, HttpResponse response) {
        return null;
    }

}
