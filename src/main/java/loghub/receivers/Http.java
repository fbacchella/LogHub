package loghub.receivers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.HttpRequestProcessing;
import loghub.processors.ParseJson;

public class Http extends GenericTcp {

    private static final ParseJson jsonParser = new ParseJson();

    public static final class HttpPrincipal implements Principal {
        private final String user;
        private final String realm;

        public HttpPrincipal(String user, String realm) {
            this.user = user;
            this.realm = realm;
        }
        @Override
        public String getName() {
            return user;
        }
        public String getRealm() {
            return realm;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((realm == null) ? 0 : realm.hashCode());
            result = prime * result + ((user == null) ? 0 : user.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            HttpPrincipal other = (HttpPrincipal) obj;
            if (realm == null) {
                if(other.realm != null)
                    return false;
            } else if (!realm.equals(other.realm))
                return false;
            if (user == null) {
                if (other.user != null)
                    return false;
            } else if (!user.equals(other.user))
                return false;
            return true;
        }
        @Override
        public String toString() {
            return user;
        }
    };

    @Sharable
    private class PostHandler extends HttpRequestProcessing {

        @Override
        public boolean acceptRequest(HttpRequest request) {
            int rmethodhash = request.method().hashCode();
            if (HttpMethod.GET.hashCode() == rmethodhash || HttpMethod.PUT.hashCode() == rmethodhash || HttpMethod.POST.hashCode() == rmethodhash) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            Principal peerPrincipal = null;
            try {
                peerPrincipal = Http.this.getSslPrincipal(Http.this.getSslSession(ctx));
            } catch (GeneralSecurityException e) {
                throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad SSL/TLS client authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
            }
            // Some authentication was defined but still not resolved
            if((Http.this.withJaas() || Http.this.encodedAuthentication != null) && peerPrincipal == null) {
                String authorization = request.headers().get(HttpHeaderNames.AUTHORIZATION);
                // No authorization header, request one
                if (authorization == null || authorization.isEmpty()) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Authentication required", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\", charset=\"UTF-8\""));
                } else {
                    if (! authorization.startsWith("BASIC ")) {
                        throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Invalid authentication scheme", Collections.emptyMap());
                    } else if (Http.this.encodedAuthentication != null && Http.this.encodedAuthentication.equals(authorization)) {
                        // Try explicit login/password in the configuration
                        peerPrincipal = new HttpPrincipal(Http.this.user, "loghub");
                    } else  if (Http.this.withJaas()) {
                        // Perhaps in jaas ?
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
                        CallbackHandler cbHandler = new CallbackHandler() {
                            @Override
                            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                                for (Callback cb: callbacks) {
                                    if (cb instanceof NameCallback) {
                                        NameCallback nc = (NameCallback)cb;
                                        nc.setName(login);
                                    } else if (cb instanceof PasswordCallback) {
                                        PasswordCallback pc = (PasswordCallback)cb;
                                        pc.setPassword(passwd);
                                    } else {
                                        throw new UnsupportedCallbackException
                                        (cb, "Unrecognized Callback");
                                    }
                                }
                            }
                        };
                        peerPrincipal = Http.this.getJaasPrincipal(cbHandler);
                        Arrays.fill(passwd, '\0');
                    }
                }
                // No authentication accepted, fails
                if (peerPrincipal == null) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
                }
            }
            // If an explicit user was given, check that it match the resolved principal
            if (Http.this.user != null && peerPrincipal != null && ! Http.this.user.equals(peerPrincipal.getName())) {
                logger.warn("failed authentication, expect {}, got {}", Http.this.user, peerPrincipal.getName());
                throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
            }
            Http.this.savePrincipal(ctx, peerPrincipal);
            Event e = Http.this.emptyEvent(Http.this.getConnectionContext(ctx, null));
            try {
                String mimeType = Optional.ofNullable(HttpUtil.getMimeType(request)).orElse("application/octet-stream").toString();
                CharSequence encoding = Optional.ofNullable(HttpUtil.getCharsetAsSequence(request)).orElse("UTF-8");
                if (request.method() == HttpMethod.GET) {
                    mimeType = "application/query-string";
                }
                String message;
                switch(mimeType) {
                case "application/json":
                case "text/plain": {
                    Charset cs = Charset.forName(encoding.toString());
                    message = request.content().toString(cs);
                    break;
                }
                case "application/x-www-form-urlencoded": {
                    Charset cs = Charset.forName(encoding.toString());
                    message = "?" + request.content().toString(cs);
                    break;
                }
                case "application/query-string":
                    message = request.uri();
                    break;
                default:
                    message = null;
                }
                switch(mimeType) {
                case "application/x-www-form-urlencoded":
                case "application/query-string": {
                    QueryStringDecoder qsd = new QueryStringDecoder(message);
                    Map<String, Object> result = qsd.parameters().entrySet().stream()
                            .collect(Collectors.toMap(i -> i.getKey(), j -> {
                                if (j.getValue().size() == 1) {
                                    return (Object) j.getValue().get(0);
                                } else {
                                    return (Object) j.getValue();
                                }
                            }) );
                    e.putAll(result);
                    break;
                }
                case "application/json":
                    e.put("__message__", message);
                    jsonParser.processMessage(e, "__message__", "message");
                    e.remove("__message__");
                    break;
                default:
                    Map<String, Object> result = Http.this.decoder.decode(e.getConnectionContext(), request.content());
                    e.putAll(result);
                }
                if (! Http.this.send(e)) {
                    throw new HttpRequestFailure(HttpResponseStatus.TOO_MANY_REQUESTS, "Busy, try again");
                };
            } catch (DecodeException | ProcessorException | IllegalCharsetNameException | UnsupportedCharsetException e1) {
                e.end();
                logger.error("Can't decode content", e1);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
            ByteBuf content = Unpooled.copiedBuffer("{'decoded': true}\r\n", StandardCharsets.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        }

        @Override
        protected String getContentType() {
            return "application/json; charset=utf-8";
        }

        @Override
        protected Date getContentDate() {
            return new Date();
        }

    };

    private final PostHandler recepter = new PostHandler();
    private final AbstractHttpServer webserver = new AbstractHttpServer() {
        @Override
        public void addHandlers(ChannelPipeline p) {
            super.addHandlers(p);
            Http.this.addHandlers(p);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addLast("recepter", recepter);
        }

    };

    private int port;
    private String host = null;
    private String user = null;
    private String password = null;
    private String encodedAuthentication = null;

    public Http(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline, null);
        setServer(webserver);
    }

    @Override
    public boolean configure(Properties properties) {
        if (user != null && password != null) {
            String authString = user + ":" + password;
            encodedAuthentication = "Basic " + Base64.getEncoder().encodeToString(authString.getBytes(StandardCharsets.UTF_8));
        }
        try {
            webserver.setPort(getPort());
            webserver.setHost(getHost());
            return super.configure(properties);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return false;
        }
    }

    @Override
    public ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> getConsummer() {
        return webserver;
    }

    @Override
    public String getReceiverName() {
        return "HTTP/" + getPort();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        // Ensure host is null if given empty string, to be resolved as "bind *" by InetSocketAddress;
        this.host = host != null && !host.isEmpty() ? host : null;
    }

    @Override
    protected ByteToMessageDecoder getSplitter() {
        return null;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Object getDecoders() {
        return null;
    }

    public void setDecoders(Object password) {
        System.out.println(password);
    }

}
