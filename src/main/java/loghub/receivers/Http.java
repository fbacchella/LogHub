package loghub.receivers;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.DecodeException.RuntimeDecodeException;
import loghub.decoders.Decoder;
import loghub.decoders.TextDecoder;
import loghub.metrics.Stats;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;
import lombok.Getter;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

@Blocking
@SelfDecoder
@BuilderClass(Http.Builder.class)
public class Http extends AbstractHttpReceiver<Http> {

    @NoCache
    @RequestAccept(methods= {"GET", "PUT", "POST"})
    @ContentType("application/json; charset=utf-8")
    private class PostHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            try {
                String mimeType = Optional.ofNullable(HttpUtil.getMimeType(request)).orElse("application/octet-stream").toString();
                if (request.method() == HttpMethod.GET) {
                    mimeType = "application/query-string";
                }
                CharSequence encoding = Optional.ofNullable(HttpUtil.getCharsetAsSequence(request)).orElse("UTF-8");
                Decoder decoder = Optional.of(mimeType).map(decoders::get).orElse(null);
                String message;
                if ("application/x-www-form-urlencoded".equals(mimeType)) {
                    Charset cs = Charset.forName(encoding.toString());
                    message = "?" + request.content().toString(cs);
                    decoder = null;
                } else if ("application/query-string".equals(mimeType)) {
                    message = request.uri();
                    decoder = null;
                } else if (decoder instanceof TextDecoder) {
                    Charset cs = Charset.forName(encoding.toString());
                    message = request.content().toString(cs);
                } else {
                    message = null;
                }
                ConnectionContext<InetSocketAddress> cctx = Http.this.getConnectionContext(ctx);
                Stream<Map<String, Object>> mapsStream;
                if (message != null && decoder == null) {
                    mapsStream = Stream.of(resolveCgi(message));
                } else if (decoder instanceof TextDecoder) {
                    mapsStream = ((TextDecoder)decoder).decode(cctx, message);
                } else if (decoder != null) {
                    mapsStream = decoders.get(mimeType).decode(cctx, request.content());
                } else {
                    throw new RuntimeDecodeException(new DecodeException("Unhandled content type " + mimeType));
                }
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                mapsStream.filter(Objects::nonNull).map(m -> Http.this.mapToEvent(cctx, m)).filter(Objects::nonNull).forEach(Http.this::send);
            } catch (DecodeException ex) {
                Http.this.manageDecodeException(ex);
                logger.error("Can't decode content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (RuntimeDecodeException ex) {
                Http.this.manageDecodeException(ex.getDecodeException());
                logger.error("Can't decode content", ex.getDecodeException());
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (IllegalCharsetNameException | UnsupportedCharsetException ex) {
                Stats.newReceivedError(Http.this, "Can't decode HTTP content: " + Helpers.resolveThrowableException(ex));
                logger.debug("Can't decode HTTP content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
            ByteBuf content = Unpooled.copiedBuffer("{\"decoded\": true}\r\n", StandardCharsets.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }

        private Map<String, Object> resolveCgi(String message) {
            QueryStringDecoder qsd = new QueryStringDecoder(message);
            return qsd.parameters().entrySet().stream()
                           .collect(Collectors.toMap(Map.Entry::getKey, j -> {
                               if (j.getValue().size() == 1) {
                                   return j.getValue().get(0);
                               } else {
                                   return j.getValue();
                               }
                           }));

        }

    }

    public static class Builder extends AbstractHttpReceiver.Builder<Http> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        @Setter
        private Map<String, Decoder> decoders = Collections.emptyMap();

        @Override
        public Http build() {
            return new Http(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final Map<String, Decoder> decoders;

    protected Http(Builder builder) {
        super(builder);
        this.decoders = Map.copyOf(new HashMap<>(builder.decoders));
        if (this.decoder != null) {
            throw new IllegalArgumentException("No default decoder can be defined");
        }
    }

    @Override
    public boolean configure(Properties properties) {
        decoders.values().forEach(d -> d.configure(properties, this));
        return super.configure(properties);
    }

    @Override
    public String getReceiverName() {
        return "HTTP/0.0.0.0/" + getPort();
    }

    @Override
    public void configureConsumer(HttpReceiverChannelConsumer.Builder<Http> builder) {
        builder.setRequestProcessor(new PostHandler());
    }

}
