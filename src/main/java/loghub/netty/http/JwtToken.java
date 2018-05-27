package loghub.netty.http;

import java.security.Principal;
import java.util.Collections;

import com.auth0.jwt.exceptions.JWTCreationException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import loghub.netty.servers.AbstractNettyServer;
import loghub.security.JWTHandler;

@NoCache
@ContentType("text/plain")
@RequestAccept(path="/token", methods={"GET"})
public class JwtToken extends HttpRequestProcessing {

    private final JWTHandler alg;

    public JwtToken(JWTHandler alg) {
        this.alg = alg;
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        Principal p = ctx.channel().attr(AbstractNettyServer.PRINCIPALATTRIBUTE).get();
        try {
            String token = alg.getToken(p);
            ByteBuf content = Unpooled.copiedBuffer(token + "\r\n", CharsetUtil.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        } catch (JWTCreationException exception){
            throw new HttpRequestFailure(HttpResponseStatus.SERVICE_UNAVAILABLE, "JWT creation failed", Collections.emptyMap());
        }
    }

}
