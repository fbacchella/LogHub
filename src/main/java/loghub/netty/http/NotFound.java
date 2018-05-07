package loghub.netty.http;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

public class NotFound extends HttpRequestProcessing implements ChannelHandler {

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        return true;
    }

}
