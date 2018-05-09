package loghub.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.security.AuthenticationHandler;

@RequestAccept(path="/token", methods={"GET"})
public class TokenFilter extends AccessControl {

    public TokenFilter(AuthenticationHandler authhandler) {
        super(authhandler);
    }

    @Override
    protected void filter(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        super.filter(request, ctx);
    }

}
