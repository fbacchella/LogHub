package loghub.netty.http;

import java.util.Date;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

@Sharable
public class NotFound extends HttpRequestProcessing implements ChannelHandler {

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
    }

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        return null;
    }

    @Override
    protected Date getContentDate(HttpRequest request, HttpResponse response) {
        return null;
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        return true;
    }

}
