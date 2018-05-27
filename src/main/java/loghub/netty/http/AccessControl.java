package loghub.netty.http;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;

import javax.net.ssl.SSLSession;
import javax.security.auth.login.FailedLoginException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.netty.servers.NettyIpServer;
import loghub.security.AuthenticationHandler;

import static loghub.netty.servers.AbstractNettyServer.PRINCIPALATTRIBUTE;

public class AccessControl extends HttpFilter {

    private final AuthenticationHandler authhandler;

    public AccessControl(AuthenticationHandler authhandler) {
        this.authhandler = authhandler;
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        return true;
    }

    @Override
    protected void filter(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        Principal peerPrincipal = null;
        try {
            SSLSession sess = ctx.channel().attr(NettyIpServer.SSLSESSIONATTRIBUTE).get();
            if (sess != null) {
                peerPrincipal = authhandler.checkSslClient(sess);
            }
        } catch (FailedLoginException e) {
            throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Incorrect SSL/TLS client authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
        } catch (GeneralSecurityException e) {
            throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid SSL/TLS client authentication", Collections.emptyMap());
        }
        if (peerPrincipal == null) {
            String authorization = request.headers().get(HttpHeaderNames.AUTHORIZATION);
            if (authorization != null && ! authorization.isEmpty()) {
                if ( authorization.toLowerCase(Locale.US).startsWith("bearer ")) {
                    if (authhandler.isWithJwt()) {
                        peerPrincipal = authhandler.checkJwt(authorization.substring(7));
                    }
                } else if ( authorization.toLowerCase(Locale.US).startsWith("basic ")) {
                    char[] content;
                    try {
                        byte[] decoded = Base64.getDecoder().decode(authorization.substring(6));
                        content = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(decoded)).array();
                    } catch (IllegalArgumentException e) {
                        logger.warn("Invalid authentication scheme: {}", e.getMessage());
                        throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid authentication scheme", Collections.emptyMap());
                    }
                    int sep = 0;
                    for ( ; sep < content.length ; sep++) {
                        if (content[sep] == ':') break;
                    }
                    String login = new String(content, 0, sep);
                    char[] passwd = Arrays.copyOfRange(content, sep + 1, content.length);
                    Arrays.fill(content, '\0');
                    peerPrincipal = authhandler.checkLoginPassword(login, passwd);
                    Arrays.fill(passwd, '\0');
                } else {
                    throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid authentication scheme", Collections.emptyMap());
                }
                // Bad login/password
                if (peerPrincipal == null) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
                }
            }
        }
        // No authorization header, request one
        if (peerPrincipal == null) {
            throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Authentication required", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
        }
        ctx.channel().attr(PRINCIPALATTRIBUTE).set(peerPrincipal);
    }

}
