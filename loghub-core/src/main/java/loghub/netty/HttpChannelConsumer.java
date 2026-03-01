package loghub.netty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2SettingsAckFrame;
import io.netty.handler.codec.http2.Http2SettingsFrame;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AttributeKey;
import loghub.Helpers;
import loghub.netty.http.AccessControl;
import loghub.netty.http.FatalErrorHandler;
import loghub.netty.http.HstsData;
import loghub.netty.http.NotFound;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.security.AuthenticationHandler;
import lombok.Setter;
import lombok.experimental.Accessors;

import static loghub.netty.transport.AbstractIpTransport.ALPNPROTOCOL;
import static loghub.netty.transport.NettyTransport.ERROR_HANDLER_NAME;

public class HttpChannelConsumer implements ChannelConsumer, AlpnResolver {

    private static final SimpleChannelInboundHandler<FullHttpRequest> NOT_FOUND = new NotFound();
    private static final SimpleChannelInboundHandler<FullHttpRequest> FATAL_ERROR = new FatalErrorHandler();
    private static final String HTTP_OBJECT_AGGREGATOR = "HttpObjectAggregator";
    public static final AttributeKey<Object> HOLDERATTRIBUTE = AttributeKey.newInstance("holder");
    public static final AttributeKey<Long> STARTTIMEATTRIBUTE = AttributeKey.newInstance("startTime");

    @Setter
    @Accessors(chain = true)
    public static class Builder {
        // Both aggregatorSupplier and serverCodecSupplier needs a supplier because
        // they are usually not sharable, so each pipeline needs its own instance.
        private Supplier<HttpObjectAggregator> aggregatorSupplier;
        private Supplier<HttpServerCodec> serverCodecSupplier;
        private Consumer<ChannelPipeline> modelSetup;
        private AuthenticationHandler authHandler;
        private int maxContentLength = 1048576;
        private Logger logger;
        private HstsData hsts;
        private Object holder;
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
    private final Consumer<ChannelPipeline> modelSetup;
    private final AuthenticationHandler authHandler;
    private final Logger logger;
    private final HstsData hsts;
    private final Object holder;

    protected HttpChannelConsumer(Builder builder) {
        this.aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElse(() -> new HttpObjectAggregator(builder.maxContentLength));
        this.serverCodecSupplier = Optional.ofNullable(builder.serverCodecSupplier).orElse(HttpServerCodec::new);
        this.modelSetup = builder.modelSetup;
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
                if (ApplicationProtocolNames.HTTP_2.equals(p)) {
                    return ApplicationProtocolNames.HTTP_2;
                } else if (ApplicationProtocolNames.HTTP_1_1.equals(p)) {
                    return ApplicationProtocolNames.HTTP_1_1;
                }
            }
            return ApplicationProtocolNames.HTTP_1_1;
        };
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.channel().attr(HOLDERATTRIBUTE).set(holder);
        p.channel().attr(STARTTIMEATTRIBUTE).set(System.nanoTime());
        p.channel().attr(ALPNPROTOCOL).set(ApplicationProtocolNames.HTTP_1_1);
    }


    @Override
    public void insertAlpnPipeline(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        LinkedHashMap<String, ChannelHandler> removed = new LinkedHashMap<>();
        // Don't use .names(), it returns context, not handlers
        List<String> names = List.copyOf(p.toMap().keySet());
        int alpnIndex = names.indexOf(RESOLVERNAME);
        if (alpnIndex == -1) {
            throw new NoSuchElementException("Aucun handler nommé 'ALPN' trouvé dans le pipeline.");
        }
        List<String> toRemove = names.subList(alpnIndex + 1, names.size());
        for (String name : toRemove) {
            ChannelHandler handler = p.remove(name);
            removed.put(name, handler);
        }
        String protocol = ctx.channel().attr(ALPNPROTOCOL).get();
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            addHttp2Handlers(p);
        } else {
            addHttp1Handlers(p);
        }
        p.remove(RESOLVERNAME);
        removed.forEach(p::addLast);
    }

    private void addHttp1Handlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", serverCodecSupplier.get());
        p.addLast("HttpContentDeCompressor", new HttpContentDecompressor());
        p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
        p.addLast("HttpContentCompressor", new HttpContentCompressor());
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        finishPipelineSetup(p);
    }

    private void addHttp2Handlers(ChannelPipeline p) {
        p.addLast("Http2FrameCodec", Http2FrameCodecBuilder.forServer().initialSettings(Http2Settings.defaultSettings()).build());
        ChannelInboundHandlerAdapter frameHandler = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                Optional<?> o = http2FrameHandling(ctx, msg);
                if (o.isPresent()) {
                    super.channelRead(ctx, o.get());
                }
            }
        };
        p.addLast("UnandledHTTP2Frame", frameHandler);
        Http2MultiplexHandler multiplexHandler = new Http2MultiplexHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline cp = ch.pipeline();
                Channel parentChannel = ch.parent();
                for (AttributeKey<?> key : List.of(
                        NettyTransport.PRINCIPALATTRIBUTE,
                        AbstractIpTransport.SSLSENGINATTRIBUTE, AbstractIpTransport.SSLSESSIONATTRIBUTE, ALPNPROTOCOL,
                        HttpChannelConsumer.HOLDERATTRIBUTE, HttpChannelConsumer.STARTTIMEATTRIBUTE
                    )
                ) {
                    copyAttribue(key, parentChannel, ch);
                }
                cp.addLast(new Http2StreamFrameToHttpObjectCodec(true, true));
                finishPipelineSetup(cp);
             }
        });
        p.addLast("Http2MultiplexHandler", multiplexHandler);
    }

    private Optional<Object> http2FrameHandling(ChannelHandlerContext ctx, Object msg) {
        switch (msg) {
        case Http2SettingsFrame hf -> {
            logger.trace("Intercepted HTTP/2 frame {}", hf::name);
            return Optional.empty();
        }
        case Http2SettingsAckFrame hf -> {
            logger.trace("Intercepted HTTP/2 frame {}", hf::name);
            return Optional.empty();
        }
        case Http2Frame hf -> {
            logger.trace("Logged HTTP/2 frame {} {} {}", () -> hf.getClass().getCanonicalName(), hf::name, () -> msg);
            return Optional.of(msg);
        }
        default -> {
            logger.info("Unknown HTTP/2 frame {}", msg);
            return Optional.of(msg);
        }
        }
    }

    private <T> void copyAttribue(AttributeKey<T> key, Channel from, Channel to) {
        if (from.hasAttr(key)) {
            T value = from.attr(key).get();
            to.attr(key).set(value);
        }
    }

    private void finishPipelineSetup(ChannelPipeline p) {
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
            modelSetup.accept(p);
        } catch (RuntimeException e) {
            logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Invalid pipeline configuration: {}", e::getMessage);
            p.addAfter(HTTP_OBJECT_AGGREGATOR, "BrokenConfigHandler", FATAL_ERROR);
        }
        p.addLast("NotFound404", NOT_FOUND);
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
