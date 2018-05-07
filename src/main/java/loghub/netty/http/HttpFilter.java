package loghub.netty.http;

import java.util.Date;
import java.util.function.Predicate;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

public abstract class HttpFilter extends HttpHandler {

    public HttpFilter() {
        super(false);
    }

    public HttpFilter(Predicate<String> urlFilter) {
        super(false, urlFilter);
    }

    public HttpFilter(Predicate<String> urlFilter, String... method) {
        super(false, urlFilter, method);
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
