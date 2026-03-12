package loghub.netty.http2;

import java.util.Collections;
import java.util.Optional;

import javax.security.auth.login.FailedLoginException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.netty.http.AccessControlHandler;
import loghub.netty.http.HttpRequestFailure;
import loghub.security.AuthenticationHandler;

public class AccessControl extends Http2Filter {

    private final AccessControlHandler accessControlHandler;

    public AccessControl(AuthenticationHandler authhandler) {
        accessControlHandler = new AccessControlHandler(authhandler, getLogger());
    }

    @Override
    public boolean acceptRequest(Http2HeadersFrame request) {
        return true;
    }

    @Override
    protected void filter(Http2HeadersFrame request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        accessControlHandler.filter(ctx.channel(), Optional.ofNullable(request.headers().get(HttpHeaderNames.AUTHORIZATION)));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof FailedLoginException) {
            throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Incorrect SSL/TLS client authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
        } else {
            ctx.fireExceptionCaught(cause);
        }
    }

}
