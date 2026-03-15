package loghub.netty.http2;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.netty.http.HttpRequestFailure;

public class NotFound extends Http2RequestProcessing {

    @Override
    protected void processRequest(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.headers().path() + " not found");
    }

    @Override
    public boolean acceptRequest(Http2HeadersFrame request) {
        return true;
    }

}
