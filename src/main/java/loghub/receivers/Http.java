package loghub.receivers;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.Stats;
import loghub.configuration.Properties;
import loghub.decoders.Decoder;
import loghub.decoders.Decoder.DecodeException;
import loghub.decoders.Decoder.RuntimeDecodeException;
import loghub.netty.AbstractTcpReceiver;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.AccessControl;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import loghub.netty.servers.AbstractNettyServer;
import lombok.Getter;
import lombok.Setter;

@Blocking(true)
@SelfDecoder
public class Http extends AbstractTcpReceiver<Http, Http.HttpReceiverServer, Http.HttpReceiverServer.Builder> {

    @NoCache
    @RequestAccept(methods= {"GET", "PUT", "POST"})
    @ContentType("application/json; charset=utf-8")
    private class PostHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            try {
                String mimeType = Optional.ofNullable(HttpUtil.getMimeType(request)).orElse("application/octet-stream").toString();
                CharSequence encoding = Optional.ofNullable(HttpUtil.getCharsetAsSequence(request)).orElse("UTF-8");
                if (request.method() == HttpMethod.GET) {
                    mimeType = "application/query-string";
                }
                String message;
                switch(mimeType) {
                case "application/json": {
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
                    if (mimeType.startsWith("text/")) {
                        Charset cs = Charset.forName(encoding.toString());
                        message = request.content().toString(cs);
                    } else {
                        message = null;
                    }
                }
                ConnectionContext<InetSocketAddress> cctx = Http.this.getConnectionContext(ctx, null);
                Stream<Map<String, Object>> mapsStream;
                if ("application/x-www-form-urlencoded".equals(mimeType)
                    || "application/query-string".equals(mimeType)) {
                    QueryStringDecoder qsd = new QueryStringDecoder(message);
                    Map<String, Object> result = qsd.parameters().entrySet().stream()
                                    .collect(Collectors.toMap(i -> i.getKey(), j -> {
                                        if (j.getValue().size() == 1) {
                                            return (Object) j.getValue().get(0);
                                        } else {
                                            return (Object) j.getValue();
                                        }
                                    }));
                    mapsStream = Stream.of(result);
                } else if (decoders.containsKey(mimeType)) {
                    mapsStream = decoders.get(mimeType).decode(cctx, request.content());
                } else {
                    throw new RuntimeDecodeException(new DecodeException("Unhandled content type " + mimeType));
                }
                Principal p = ctx.channel().attr(AbstractNettyServer.PRINCIPALATTRIBUTE).get();
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                mapsStream.map(m -> Http.this.mapToEvent(cctx, () -> ! m.isEmpty(), () -> m)).forEach(Http.this::send);
            } catch (DecodeException ex) {
                Http.this.manageDecodeException(ex);
                logger.error("Can't decode content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (RuntimeDecodeException ex) {
                Http.this.manageDecodeException(ex.getDecodeException());
                logger.error("Can't decode content", ex.getDecodeException());
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (IllegalCharsetNameException | UnsupportedCharsetException ex) {
                Stats.newReceivedError("Can't decode HTTP content: " + Helpers.resolveThrowableException(ex));
                logger.debug("Can't decode HTTP content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
            ByteBuf content = Unpooled.copiedBuffer("{'decoded': true}\r\n", StandardCharsets.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }

    };

    protected static class HttpReceiverServer extends AbstractHttpServer<HttpReceiverServer, HttpReceiverServer.Builder> {

        protected static class Builder extends AbstractHttpServer.Builder<HttpReceiverServer, Builder> {
            HttpRequestProcessing receiver;
            Builder setReceiveHandler(HttpRequestProcessing recepter) {
                this.receiver = recepter;
                return this;
            }
            @Override
            public HttpReceiverServer build() throws IllegalArgumentException, InterruptedException {
                return new HttpReceiverServer(this);
            }
        }

        final HttpRequestProcessing receiver;
        protected HttpReceiverServer(Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
            this.receiver = builder.receiver;
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            if (getAuthHandler() != null) {
                p.addLast("authentication", new AccessControl(getAuthHandler()));
                logger.debug("Added authentication");
            }
            p.addLast("receiver", receiver);
        }
    }

    @Getter @Setter
    private Map<String, Decoder> decoders = Collections.emptyMap();

    public Http() {
        super();
    }

    @Override
    protected HttpReceiverServer.Builder getServerBuilder() {
        return new HttpReceiverServer.Builder();
    }

    @Override
    public boolean configure(Properties properties, HttpReceiverServer.Builder builder) {
        decoders.values().forEach(d -> d.configure(properties, this));
        settings(builder);
        return super.configure(properties, builder);
    }

    protected void settings(HttpReceiverServer.Builder builder) {
        builder.setReceiveHandler(new PostHandler()).setThreadPrefix("HTTP");
    }

    @Override
    public String getReceiverName() {
        return "HTTP/" + getPort();
    }

    @Override
    public void setDecoder(Decoder codec) {
        throw new IllegalArgumentException("No default decoder can be defined");
    }

}
