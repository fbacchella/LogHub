package loghub.receivers;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.BuildableConnectionContext;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.DecodeException.RuntimeDecodeException;
import loghub.decoders.Decoder;
import loghub.decoders.TextDecoder;
import loghub.events.Event;
import loghub.metrics.Stats;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.ContextExtractorGeneric;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;
import loghub.types.MimeType;
import lombok.Getter;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

@Getter
@Blocking
@SelfDecoder
@BuilderClass(Http.Builder.class)
public class Http extends AbstractHttpReceiver<Http, Http.Builder> {

    private static final MimeType APPLICATION_OCTET_STREAM = MimeType.of("application/octet-stream");
    private static final MimeType APPLICATION_QUERY_STRING = MimeType.of("application/query-string");
    private static final MimeType APPLICATION_FORM_URLENCODED = MimeType.of("application/x-www-form-urlencoded");

    @NoCache
    @RequestAccept(methods = { "GET", "PUT", "POST" })
    @ContentType("application/json; charset=utf-8")
    private class PostHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            try {
                MimeType mimeType;
                if (request.method() == HttpMethod.GET) {
                    mimeType = APPLICATION_QUERY_STRING;
                } else {
                    mimeType = Optional.of(request.headers())
                                       .map(h -> h.get(HttpHeaderNames.CONTENT_TYPE))
                                       .map(MimeType::of)
                                       .orElse(APPLICATION_OCTET_STREAM);
                }
                Decoder decoder = Optional.of(mimeType).map(decoders::get).orElse(null);
                String message;
                if (APPLICATION_FORM_URLENCODED.typeEquals(mimeType)) {
                    Charset cs = mimeType.getCharset().orElse(StandardCharsets.UTF_8);
                    message = "?" + request.content().toString(cs);
                    decoder = null;
                } else if (APPLICATION_QUERY_STRING.typeEquals(mimeType)) {
                    message = request.uri();
                    decoder = null;
                } else if (decoder instanceof TextDecoder) {
                    Charset cs = mimeType.getCharset().orElse(StandardCharsets.UTF_8);
                    message = request.content().toString(cs);
                } else {
                    message = null;
                }
                BuildableConnectionContext<InetSocketAddress> cctx = Http.this.getConnectionContext(ctx);
                Stream<Map<String, Object>> mapsStream;
                if (message != null && decoder == null) {
                    mapsStream = Stream.of(resolveCgi(message));
                } else if (decoder instanceof TextDecoder td) {
                    mapsStream = td.decode(cctx, message);
                } else if (decoder != null) {
                    mapsStream = decoders.get(mimeType).decode(cctx, request.content());
                } else {
                    throw new RuntimeDecodeException(new DecodeException("Unhandled content type " + mimeType));
                }
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                mapsStream.filter(Objects::nonNull)
                          .map(m -> Http.this.mapToEvent(cctx, m))
                          .filter(Objects::nonNull)
                          .forEach(Http.this::send);
            } catch (DecodeException ex) {
                Http.this.manageDecodeException(ex);
                logger.error("Can't decode content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (RuntimeDecodeException ex) {
                Http.this.manageDecodeException(ex.getDecodeException());
                logger.error("Can't decode content", ex.getDecodeException());
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            } catch (IllegalCharsetNameException | UnsupportedCharsetException ex) {
                Stats.newReceivedError(Http.this,
                        "Can't decode HTTP content: " + Helpers.resolveThrowableException(ex));
                logger.debug("Can't decode HTTP content", ex);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
            ByteBuf content = Unpooled.copiedBuffer("{\"decoded\": true}\r\n", StandardCharsets.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }
    }

    @Setter
    public static class Builder extends AbstractHttpReceiver.Builder<Http, Http.Builder> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        private Map<String, Decoder> decoders = Collections.emptyMap();

        @Override
        public Http build() {
            return new Http(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Map<MimeType, Decoder> decoders;

    protected Http(Builder builder) {
        super(builder);
        this.decoders = resolverDecoders(builder.decoders);
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "NettyHTTPReceiver";
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
    protected void modelSetup(ChannelPipeline pipeline) {
        pipeline.addLast(new PostHandler());
    }

    @Override
    protected void configureConsumer(HttpChannelConsumer.Builder builder) {
        super.configureConsumer(builder);
        builder.setHttp2handler(this::consumeHttp2);
    }

    private void consumeHttp2(Channel ch) {
        AtomicReference<Http2RequestStat> channelStatHolder = new AtomicReference<>();
        ch.pipeline().addLast(ContextExtractorGeneric.NAME, http2resolver);
        ch.pipeline().addLast("StreamConsumer", new SimpleChannelInboundHandler<Http2Frame>() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, Http2Frame msg) {
                switch (msg) {
                case Http2HeadersFrame hf -> onHeadersReceived(ctx, hf, channelStatHolder);
                case Http2DataFrame df -> onDataReceived(ctx, df, channelStatHolder);
                default -> ctx.fireChannelRead(msg);
                }
            }
        });
    }

    private record Http2RequestStat(MimeType mimeType, Charset charset, Decoder decoder) {}

    private void onHeadersReceived(ChannelHandlerContext ctx, Http2HeadersFrame frame, AtomicReference<Http2RequestStat> channelStat) {
        Http2Headers headers = frame.headers();
        MimeType mimeType;
        if ("GET".equalsIgnoreCase(headers.method().toString())) {
            Optional.ofNullable(headers.path())
                    .ifPresent(p -> sendCgi(ctx, p.toString()));
            if (frame.isEndStream()) {
                Http2Headers responseHeaders = new DefaultHttp2Headers().status("200");
                ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, true));
            }
        } else {
            mimeType = Optional.of(headers)
                               .map(h -> h.get(HttpHeaderNames.CONTENT_TYPE))
                               .map(cs -> MimeType.of(cs.toString()))
                               .orElse(APPLICATION_OCTET_STREAM);
            Charset charset = mimeType.getCharset().orElse(StandardCharsets.UTF_8);
            Decoder decoder = Optional.of(mimeType).map(decoders::get).orElse(null);
            channelStat.set(new Http2RequestStat(mimeType, charset, decoder));
        }
    }

    private void sendCgi(ChannelHandlerContext ctx, String message) {
        Map<String, Object> map = resolveCgi(message);
        BuildableConnectionContext<InetSocketAddress> cctx = Http.this.getConnectionContext(ctx);
        Event ev = mapToEvent(cctx, map);
        send(ev);
    }

    private void onDataReceived(ChannelHandlerContext ctx, Http2DataFrame frame, AtomicReference<Http2RequestStat> channelStatHolder) {
        Http2RequestStat channelStat = channelStatHolder.get();
        if (APPLICATION_FORM_URLENCODED.typeEquals(channelStat.mimeType)) {
            Charset cs = channelStat.mimeType.getCharset().orElse(StandardCharsets.UTF_8);
            String message = "?" + frame.content().toString(cs);
            sendCgi(ctx, message);
        } else if (channelStat.decoder != null) {
            try {
                Decoder decoder = channelStat.decoder;
                BuildableConnectionContext<InetSocketAddress> cctx = Http.this.getConnectionContext(ctx);
                Stream<Map<String, Object>> mapsStream = decoder.decode(cctx, frame.content());
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                mapsStream.filter(Objects::nonNull).map(m -> Http.this.mapToEvent(cctx, m)).filter(Objects::nonNull)
                        .forEach(Http.this::send);
            } catch (DecodeException ex) {
                Http.this.manageDecodeException(ex);
                logger.error("Can't decode content", ex);
            }
        }
        if (frame.isEndStream()) {
            Http2Headers responseHeaders = new DefaultHttp2Headers().status("200");
            ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, true));
        }
    }

    private Map<String, Object> resolveCgi(String message) {
        QueryStringDecoder qsd = new QueryStringDecoder(message);
        return qsd.parameters()
                  .entrySet()
                  .stream()
                  .collect(Collectors.toMap(
                      Map.Entry::getKey,
                      j -> j.getValue().size() == 1 ? j.getValue().getFirst() : j.getValue())
                  );
    }

}
