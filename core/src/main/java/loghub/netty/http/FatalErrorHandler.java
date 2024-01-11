package loghub.netty.http;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

@ChannelHandler.Sharable
public class FatalErrorHandler extends HttpRequestProcessing {
    @Override
    public boolean acceptRequest(HttpRequest request) {
        return true;
    }
    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        throw new HttpRequestFailure(HttpResponseStatus.SERVICE_UNAVAILABLE, "Unable to process request because of invalid configuration");
    }
    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        return null;
    }
}
