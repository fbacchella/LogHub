package loghub.netty.http;

import java.util.Date;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class RootRedirect extends HttpRequestProcessing implements ChannelHandler {

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.equals("/");
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.MOVED_PERMANENTLY);
        boolean keepAlive = addKeepAlive(request, response, 0);
        response.headers().set(HttpHeaderNames.LOCATION, "/static/index.html");
        ChannelFuture redirectfutur = ctx.writeAndFlush(response);
        addLogger(redirectfutur, request.method().name(), request.uri(), response.status().code(), "redirect");
        return keepAlive;
    }

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        throw new UnsupportedOperationException("No content type for redirect");
    }

    @Override
    protected Date getContentDate(HttpRequest request, HttpResponse response) {
        throw new UnsupportedOperationException("No date for redirect");
    }

}
