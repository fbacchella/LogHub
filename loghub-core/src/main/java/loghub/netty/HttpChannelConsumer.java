package loghub.netty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2PingFrame;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2SettingsAckFrame;
import io.netty.handler.codec.http2.Http2SettingsFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import loghub.Helpers;
import loghub.netty.http.AccessControl;
import loghub.netty.http.FatalErrorHandler;
import loghub.netty.http.HstsData;
import loghub.netty.http.HttpProtocolVersion;
import loghub.netty.http.NotFound;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.security.AuthenticationHandler;
import lombok.Setter;
import lombok.experimental.Accessors;

import static loghub.netty.transport.AbstractIpTransport.ALPNPROTOCOL;
import static loghub.netty.transport.NettyTransport.ERROR_HANDLER_NAME;

public class HttpChannelConsumer implements ChannelConsumer, AlpnResolver {

    private static final SimpleChannelInboundHandler<FullHttpRequest> NOT_FOUND_HTTP_1_1 = new NotFound();
    private static final SimpleChannelInboundHandler<Http2HeadersFrame> NOT_FOUND_HTTP_2 = new loghub.netty.http2.NotFound();
    private static final SimpleChannelInboundHandler<FullHttpRequest> FATAL_ERROR = new FatalErrorHandler();
    private static final String HTTP_OBJECT_AGGREGATOR = "HttpObjectAggregator";
    public static final AttributeKey<Object> HOLDERATTRIBUTE = AttributeKey.newInstance("holder");
    public static final AttributeKey<Long> STARTTIMEATTRIBUTE = AttributeKey.newInstance("startTime");

    private class PipeLineStatHolder {
        final LinkedHashMap<String, ChannelHandler> removed;
        final ChannelPipeline pipeline;
        PipeLineStatHolder(ChannelHandlerContext ctx) {
            removed = new LinkedHashMap<>();
            this.pipeline = ctx.pipeline();
            // Don't use .names(), it returns context, not handlers
            List<String> names = List.copyOf(pipeline.toMap().keySet());
            int currentIndex = names.indexOf(ctx.name());
            if (currentIndex >= 0) {
                List<String> toRemove = names.subList(currentIndex + 1, names.size());
                for (String n: toRemove) {
                    ChannelHandler handler = pipeline.remove(n);
                    removed.put(n, handler);
                }
            }
        }
        void putback() {
            removed.forEach(pipeline::addLast);
        }
    }

    private class LogHubFrameHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            switch (msg) {
            case Http2SettingsFrame hf -> logger.trace("Intercepted HTTP/2 frame {}", hf::name);
            case Http2SettingsAckFrame hf -> logger.trace("Intercepted HTTP/2 frame {}", hf::name);
            case Http2PingFrame ping -> {
                logger.trace("Intercepted HTTP/2 PING frame: ack={}", ping::ack);
                if (ping.ack()) {
                    super.channelRead(ctx, msg);
                }
            }
            case Http2GoAwayFrame goaway -> {
                logger.trace("Intercepted HTTP/2 GOAWAY frame: lastStreamId={}, errorCode={}, debugData={}",
                        goaway::lastStreamId, goaway::errorCode, () -> goaway.content().toString(CharsetUtil.UTF_8));
                ctx.close();
                super.channelRead(ctx, msg);
            }
            case Http2Frame hf -> {
                logger.trace("Logged HTTP/2 frame {} {} {}", () -> hf.getClass().getCanonicalName(), hf::name, () -> msg);
                super.channelRead(ctx, msg);
            }
            default -> {
                logger.info("Unknown HTTP/2 frame {}", msg);
                super.channelRead(ctx, msg);
            }
            }
        }
    }

    private class LogHubMultiplexChannelInitializer extends ChannelInitializer<Http2StreamChannel> {
        @Override
        protected void initChannel(Http2StreamChannel ch) {
            Channel parentChannel = ch.parent();
            for (AttributeKey<?> key : List.of(
                    NettyTransport.PRINCIPALATTRIBUTE, NettyReceiver.CONNECTIONCONTEXTATTRIBUTE,
                    AbstractIpTransport.SSLSENGINATTRIBUTE, AbstractIpTransport.SSLSESSIONATTRIBUTE, AbstractIpTransport.ALPNPROTOCOL,
                    HttpChannelConsumer.HOLDERATTRIBUTE, HttpChannelConsumer.STARTTIMEATTRIBUTE
                )
            ) {
                copyAttribue(key, parentChannel, ch);
            }
            if (http1only) {
                ch.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true, true));
                finishHttp1PipelineSetup(ch.pipeline());
            } else {
                finishHttp2PipelineSetup(ch.pipeline());
            }
        }
    }

    private class LogHubHttp1Message extends SimpleChannelInboundHandler<HttpMessage> {
        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpMessage msg) {
            ChannelPipeline pipeline = ctx.pipeline();
            PipeLineStatHolder state = new PipeLineStatHolder(ctx);
            addHttp1Handlers(pipeline);
            pipeline.remove(this);
            state.putback();
            ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
        }
    }

    private class FilteredHttp2ServerUpgradeHandler extends HttpServerUpgradeHandler {
        public FilteredHttp2ServerUpgradeHandler(HttpServerCodec sourceCodec, UpgradeCodecFactory upgradeCodecFactory) {
            super(sourceCodec, upgradeCodecFactory);
        }

        @Override
        protected boolean shouldHandleUpgradeRequest(HttpRequest req) {
            // It's probably dangerous to upgrade after ony else than a PUT
            // It know to fails on POST
            return ! req.method().equals(HttpMethod.GET) ? false : super.shouldHandleUpgradeRequest(req);
        }
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {
        // Both aggregatorSupplier and serverCodecSupplier needs a supplier because
        // they are usually not sharable, so each pipeline needs its own instance.
        private Supplier<HttpObjectAggregator> aggregatorSupplier;
        private Supplier<HttpServerCodec> serverCodecSupplier;
        private Consumer<ChannelPipeline> modelSetup;
        private Consumer<Channel> http2handler;
        private BiConsumer<HttpProtocolVersion, ChannelPipeline> versionedModelSetup;
        private AuthenticationHandler authHandler;
        private int maxContentLength = 1048576;
        private Logger logger;
        private HstsData hsts;
        private Object holder;
        private boolean http1only = false;

        public Builder setVersionedModelSetup(BiConsumer<HttpProtocolVersion, ChannelPipeline> versionedModelSetup) {
            this.versionedModelSetup = versionedModelSetup;
            this.modelSetup = null;
            this.http2handler = null;
            return this;
        }

        private Builder() {

        }
        public HttpChannelConsumer build() {
            return new HttpChannelConsumer(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Supplier<HttpObjectAggregator> aggregatorSupplier;
    private final Supplier<HttpServerCodec> serverCodecSupplier;
    private final BiConsumer<HttpProtocolVersion, ChannelPipeline> modelSetup;
    private final AuthenticationHandler authHandler;
    private final Logger logger;
    private final HstsData hsts;
    private final Object holder;
    private final boolean http1only;

    protected HttpChannelConsumer(Builder builder) {
        this.aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElse(() -> new HttpObjectAggregator(builder.maxContentLength));
        this.serverCodecSupplier = Optional.ofNullable(builder.serverCodecSupplier).orElse(HttpServerCodec::new);
        if (builder.versionedModelSetup != null) {
            this.modelSetup = builder.versionedModelSetup;
            this.http1only = builder.http1only;
        } else if (builder.modelSetup != null && builder.http2handler != null) {
            this.modelSetup = (v, p) -> {
                switch (v) {
                case HttpProtocolVersion.HTTP_1_1 -> builder.modelSetup.accept(p);
                case HttpProtocolVersion.HTTP_2 -> builder.http2handler.accept(p.channel());
                default -> throw new IllegalStateException("Unexpected value: " + v);
                }
            };
            this.http1only = false;
        } else if (builder.modelSetup != null) {
            this.modelSetup = (v, c) -> {
                if (v != HttpProtocolVersion.HTTP_1_1) {
                    throw new IllegalArgumentException("Only handle HTTP/1_1");
                } else {
                    builder.modelSetup.accept(c);
                }
            };
            this.http1only = true;
        } else if (builder.http2handler != null){
            this.modelSetup = (v, c) -> {
                if (v != HttpProtocolVersion.HTTP_2) {
                    throw new IllegalArgumentException("Only handle HTTP/2");
                } else {
                    builder.http2handler.accept(c.channel());
                }
            };
            this.http1only = false;
        } else {
            throw new IllegalArgumentException("No model setup handling provided");
        }
        this.authHandler = builder.authHandler;
        this.logger = Optional.ofNullable(builder.logger).orElseGet(this::getDefaultLogger);
        this.hsts = builder.hsts;
        this.holder = builder.holder;
    }

    private Logger getDefaultLogger() {
        return LogManager.getLogger();
    }

    public BiFunction<SSLEngine, List<String>, String> getAlpnSelector() {
        return (e, l) -> {
            for (String p: l) {
                if (HttpProtocolVersion.fromAlpnId(p).isPresent()) {
                    return p;
                }
            }
            return ApplicationProtocolNames.HTTP_1_1;
        };
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.channel().attr(HOLDERATTRIBUTE).set(holder);
        p.channel().attr(STARTTIMEATTRIBUTE).set(System.nanoTime());
    }

    @Override
    public void insertAlpnPipeline(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        PipeLineStatHolder state = new PipeLineStatHolder(ctx);
        String protocol = ctx.channel().attr(ALPNPROTOCOL).get();
        if (protocol == null) {
            upgrader(p);
        } else {
            logger.debug("Negotiated ALPN protocol {}", protocol);
            switch (HttpProtocolVersion.fromAlpnId(protocol).orElse(null)) {
            case HttpProtocolVersion.HTTP_1_1 -> addHttp1Handlers(p);
            case HttpProtocolVersion.HTTP_2 -> addHttp2Handlers(p);
            default -> throw new IllegalStateException("Unexpected value: " + HttpProtocolVersion.fromAlpnId(protocol));
            }
        }
        state.putback();
    }

    private void addHttp1Handlers(ChannelPipeline p) {
        p.addLast("HttpContentDeCompressor", new HttpContentDecompressor());
        p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
        p.addLast("HttpContentCompressor", new HttpContentCompressor());
        finishHttp1PipelineSetup(p);
    }

    private void upgrader(ChannelPipeline p) {
        HttpServerCodec sourceCodec = serverCodecSupplier.get();
        HttpServerUpgradeHandler.UpgradeCodecFactory upgradeCodecFactory =
                protocol -> {
                    if (Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME.equals(protocol)) {
                        logger.debug("Upgrade to protocol {} detected", protocol);
                        return getUpgradeCodec();
                    }
                    return null;
                };
        HttpServerUpgradeHandler upgradeHandler = new FilteredHttp2ServerUpgradeHandler(sourceCodec, upgradeCodecFactory);
        CleartextHttp2ServerUpgradeHandler cleartextHandler =
                new CleartextHttp2ServerUpgradeHandler(
                        sourceCodec,
                        upgradeHandler,
                        getHttp2ServerHandler()
                );
        p.addLast(cleartextHandler);
        p.addLast(new LogHubHttp1Message());
    }

    private Http2ServerUpgradeCodec getUpgradeCodec() {
        Http2FrameCodec fcodec = Http2FrameCodecBuilder.forServer().initialSettings(Http2Settings.defaultSettings()).autoAckPingFrame(true).build();
        ChannelInboundHandlerAdapter frameHandler = new LogHubFrameHandler();
        Http2MultiplexHandler multiplexHandler = new Http2MultiplexHandler(new LogHubMultiplexChannelInitializer());
        return new Http2ServerUpgradeCodec(fcodec, frameHandler, multiplexHandler);
    }

    ChannelHandler getHttp2ServerHandler() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline p = ch.pipeline();
                addHttp2Handlers(p);
            }
        };
    }

    private void addHttp2Handlers(ChannelPipeline p) {
        p.addLast("Http2FrameCodec", Http2FrameCodecBuilder.forServer().initialSettings(Http2Settings.defaultSettings()).autoAckPingFrame(true).build());
        p.addLast("UnandledHTTP2Frame", new LogHubFrameHandler());
        Http2MultiplexHandler multiplexHandler = new Http2MultiplexHandler(new LogHubMultiplexChannelInitializer());
        p.addLast("Http2MultiplexHandler", multiplexHandler);
    }

    private <T> void copyAttribue(AttributeKey<T> key, Channel from, Channel to) {
        if (from.hasAttr(key)) {
            T value = from.attr(key).get();
            to.attr(key).set(value);
        }
    }

    private void finishHttp1PipelineSetup(ChannelPipeline p) {
        p.addLast(new ChunkedWriteHandler());
        p.addLast(HTTP_OBJECT_AGGREGATOR, aggregatorSupplier.get());
        if (authHandler != null) {
            p.addLast("Authentication", new AccessControl(authHandler));
            logger.debug("Added authentication");
        }
        if (hsts != null) {
            p.addLast("HstsHandler", hsts.getChannelHandler());
            logger.debug("Added HSTS header");
        }
        try {
            modelSetup.accept(HttpProtocolVersion.HTTP_1_1, p);
        } catch (RuntimeException e) {
            logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Invalid pipeline configuration: {}", e::getMessage);
            p.addAfter(HTTP_OBJECT_AGGREGATOR, "BrokenConfigHandler", FATAL_ERROR);
        }
        p.addLast("NotFound404", NOT_FOUND_HTTP_1_1);
    }

    private void finishHttp2PipelineSetup(ChannelPipeline p) {
        if (hsts != null) {
            p.addLast("HstsHandler", hsts.getChannelHandler());
            logger.debug("Added HSTS header");
        }
        if (authHandler != null) {
            p.addLast("Authentication", new loghub.netty.http2.AccessControl(authHandler));
            logger.debug("Added authentication");
        }
        try {
            modelSetup.accept(HttpProtocolVersion.HTTP_2, p);
        } catch (RuntimeException e) {
            logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Invalid pipeline configuration: {}", e::getMessage);
            p.addAfter(HTTP_OBJECT_AGGREGATOR, "BrokenConfigHandler", FATAL_ERROR);
        }
        p.addLast("NotFound404", NOT_FOUND_HTTP_2);
    }

    @Override
    public void addOptions(ServerBootstrap bootstrap) {
        ChannelConsumer.super.addOptions(bootstrap);
    }

    @Override
    public void addOptions(Bootstrap bootstrap) {
        ChannelConsumer.super.addOptions(bootstrap);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) {
        logger.atError().withThrowable(logger.isDebugEnabled() ? cause : null).log("Unable to process query: {}", () -> Helpers.resolveThrowableException(cause));
        ctx.pipeline().addBefore(ERROR_HANDLER_NAME, "FatalErrorHandler", FATAL_ERROR);
    }

    @Override
    public void logFatalException(Throwable ex) {
        logger.fatal("Caught fatal exception", ex);
    }

}
