package loghub.netty.http;

import java.util.Date;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

public class NotFound extends HttpStreaming implements ChannelHandler {

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
    }

    @Override
    protected String getContentType() {
        return null;
    }

    @Override
    protected Date getContentDate() {
        return null;
    }

    @Override
    protected long getSize() {
        return 0;
    }

}
