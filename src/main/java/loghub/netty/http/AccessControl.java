package loghub.netty.http;

import static loghub.netty.servers.AbstractNettyServer.PRINCIPALATTRIBUTE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.FailedLoginException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.security.AuthenticationHandler;

public class AccessControl extends HttpFilter {

    private static final Pattern AUTHDETAILS = Pattern.compile("(?<scheme>\\p{ASCII}+) (?<value>.+)");

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
        Principal peerPrincipal = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
        if (peerPrincipal != null) {
            Principal pp = peerPrincipal;
            logger.debug("Already extracted principal: \"{}\"", () -> pp.getName());
            //not null, someone (probably TLS) already done the job, nice !
            return;
        }
        if (peerPrincipal == null) {
            //String authorization = ;
            Matcher matcher = Optional.ofNullable(request.headers().get(HttpHeaderNames.AUTHORIZATION)).map(AUTHDETAILS::matcher).filter(Matcher::matches).orElse(null);
            if (matcher!= null) {
                switch(matcher.group("scheme").toLowerCase(Locale.US)) {
                case "bearer":
                    if (authhandler.isWithJwt()) {
                        peerPrincipal = authhandler.checkJwt(matcher.group("value"));
                    }
                    break;
                case "basic": {
                    char[] content;
                    try {
                        byte[] decoded = Base64.getDecoder().decode(matcher.group("value"));
                        content = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(decoded)).array();
                        Arrays.fill(decoded, (byte)0);
                    } catch (IllegalArgumentException e) {
                        logger.warn("Invalid basic authentication scheme details: {}", e.getMessage());
                        throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid basic authentication scheme details", Collections.emptyMap());
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
                    break;
                }
                default:
                    throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Unknown authentication scheme " + matcher.group("scheme"), Collections.emptyMap());
                }
                // Bad login/password
                if (peerPrincipal == null) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
                }
                Principal pp = peerPrincipal;
                logger.debug("Principal resolved as \"{}\" using scheme {}", () -> pp.getName(), () -> matcher.group("scheme"));
            } else {
                // We found an Authorization header but it was unusable
                if (request.headers().get(HttpHeaderNames.AUTHORIZATION) != null) {
                    throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid authentication header", Collections.emptyMap());
                }
            }
        }
        // No authorization header, request one
        if (peerPrincipal == null) {
            throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Authentication required", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
        }
        ctx.channel().attr(PRINCIPALATTRIBUTE).set(peerPrincipal);
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
