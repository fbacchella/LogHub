package loghub.netty.http;

import java.util.Date;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

@RequestAccept(path="/")
public class RootRedirect extends HttpRequestProcessing implements ChannelHandler {

    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        writeResponse(ctx, request, HttpResponseStatus.MOVED_PERMANENTLY, Unpooled.EMPTY_BUFFER, 0);
    }

    @Override
    protected void addCustomHeaders(HttpRequest request,
                                    HttpResponse response) {
        response.headers().set(HttpHeaderNames.LOCATION, "/static/index.html");
    }

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        return null;
    }

    @Override
    protected Date getContentDate(HttpRequest request, HttpResponse response) {
        return null;
    }

}
