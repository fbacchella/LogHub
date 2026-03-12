package loghub.netty.http;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.security.AuthenticationHandler;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

public class AccessControlHandler {

    private static final Pattern AUTHDETAILS = Pattern.compile("(?<scheme>\\p{Alnum}+)\\h+(?<value>.+)");

    private final AuthenticationHandler authhandler;
    private final Logger logger;

    public AccessControlHandler(AuthenticationHandler authhandler, Logger logger) {
        this.authhandler = authhandler;
        this.logger = logger;
    }

    public void filter(Channel channel, Optional<CharSequence> authorizationHeader) throws HttpRequestFailure {
        Principal peerPrincipal = channel.attr(PRINCIPALATTRIBUTE).get();
        if (peerPrincipal != null) {
            logger.debug("Already extracted principal: \"{}\"", peerPrincipal::getName);
            // not null, someone (probably TLS) already done the job, nice !
            return;
        }
        Matcher matcher = authorizationHeader.map(AUTHDETAILS::matcher).filter(Matcher::matches).orElse(null);
        if (matcher != null) {
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
                    Arrays.fill(decoded, (byte) 0);
                } catch (IllegalArgumentException e) {
                    logger.warn("Invalid basic authentication scheme details: {}", e.getMessage());
                    throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid basic authentication scheme details", Collections.emptyMap());
                }
                int sep = 0;
                for ( ; sep < content.length; sep++) {
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
            logger.debug("Principal resolved as \"{}\" using scheme {}", pp::getName, () -> matcher.group("scheme"));
        } else {
            // We found an Authorization header but it was unusable
            if (authorizationHeader.isPresent()) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid authentication header", Map.of());
            }
        }
        // No authorization header, request one
        if (peerPrincipal == null) {
            throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Authentication required", Map.of(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
        }
        channel.attr(PRINCIPALATTRIBUTE).set(peerPrincipal);
    }

}
