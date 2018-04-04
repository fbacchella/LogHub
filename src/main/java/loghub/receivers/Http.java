package loghub.receivers;

import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Level;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.HttpRequestProcessing;
import loghub.processors.ParseJson;

public class Http extends GenericTcp {

    private static final ParseJson jsonParser = new ParseJson();

    private static enum ClientAuthentication {
        REQUIRED,
        WANTED,
        NOTNEEDED,
    };

    private static final AttributeKey<SSLSession> sessattr = AttributeKey.newInstance(SSLSession.class.getName());

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
            String user = null;
            if (Http.this.withSsl) {
                try {
                    SSLSession sess = ctx.channel().attr(sessattr).get();
                    if (Http.this.sslclient == ClientAuthentication.WANTED || Http.this.sslclient == ClientAuthentication.REQUIRED) {
                        Principal p = sess.getPeerPrincipal();
                        if (p != null) {
                            user = p.getName();
                        }
                    }
                } catch (SSLPeerUnverifiedException e1) {
                    if (Http.this.sslclient == ClientAuthentication.REQUIRED) {
                        throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
                    }
                }
            }
            if (Http.this.encodedAuthentication != null && user == null) {
                String authorization = request.headers().get(HttpHeaderNames.AUTHORIZATION);
                if (authorization == null) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Authentication required", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
                } else if (! Http.this.encodedAuthentication.equals(authorization)) {
                    throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
                } else {
                    user = Http.this.user;
                }
            }
            if (Http.this.user != null && ! Http.this.user.equals(user)) {
                logger.warn("failed authentication, expect {}, got {}", Http.this.user, user);
                throw new HttpRequestFailure(HttpResponseStatus.UNAUTHORIZED, "Bad authentication", Collections.singletonMap(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"loghub\""));
            }
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
            if (Http.this.sslctx != null) {
                SSLEngine engine = Http.this.sslctx.createSSLEngine();
                engine.setUseClientMode(false);
                if (Http.this.sslclient == ClientAuthentication.REQUIRED) {
                    engine.setNeedClientAuth(true);
                } else if (Http.this.sslclient == ClientAuthentication.WANTED){
                    engine.setWantClientAuth(true);
                }
                SslHandler sslHandler = new SslHandler(engine);
                p.addFirst("ssl", sslHandler);
                Future<Channel> future = sslHandler.handshakeFuture();
                future.addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
                        try {
                            future.get().attr(sessattr).set(sslHandler.engine().getSession());
                        } catch (ExecutionException e) {
                            logger.warn("Failed ssl connexion", e.getMessage());
                            logger.catching(Level.DEBUG, e);
                        }
                    }});
            }
            super.addHandlers(p);
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
    private boolean withSsl = false;
    private SSLContext sslctx = null;
    private ClientAuthentication sslclient = ClientAuthentication.NOTNEEDED;

    public Http(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
        setServer(webserver);
    }

    @Override
    public boolean configure(Properties properties) {
        if (user != null && password != null) {
            String authString = user + ":" + password;
            encodedAuthentication = "Basic " + Base64.getEncoder().encodeToString(authString.getBytes(StandardCharsets.UTF_8));
        }
        if (withSsl) {
            sslctx = properties.ssl;
        }
        try {
            webserver.setPort(getPort());
            webserver.setHost(getHost());
            return webserver.configure(properties) && super.configure(properties);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return false;
        }
    }

    @Override
    public void run() {
        webserver.finish();
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

    /**
     * @return the withSsl
     */
    public boolean isWithSsl() {
        return withSsl;
    }

    /**
     * @param withSsl the withSsl to set
     */
    public void setWithSsl(boolean withSsl) {
        this.withSsl = withSsl;
    }

    /**
     * @return the sslclient
     */
    public String getSslClientAuthentication() {
        return sslclient.name().toLowerCase();
    }

    /**
     * @param sslclient the sslclient to set
     */
    public void setSslClientAuthentication(String sslclient) {
        this.sslclient = ClientAuthentication.valueOf(sslclient.toUpperCase());
    }

}
