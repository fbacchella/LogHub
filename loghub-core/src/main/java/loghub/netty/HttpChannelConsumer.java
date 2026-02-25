package loghub.netty;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLEngine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
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

public class HttpChannelConsumer implements ChannelConsumer {

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
        private Consumer<ChannelPipeline> modelSetup2;
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
    private final Consumer<ChannelPipeline> modelSetup2;
    private final AuthenticationHandler authHandler;
    private final Logger logger;
    private final HstsData hsts;
    private final Object holder;

    protected HttpChannelConsumer(Builder builder) {
        this.aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElse(() -> new HttpObjectAggregator(builder.maxContentLength));
        this.serverCodecSupplier = Optional.ofNullable(builder.serverCodecSupplier).orElse(HttpServerCodec::new);
        this.modelSetup = builder.modelSetup;
        this.modelSetup2 = Optional.ofNullable(builder.modelSetup2).orElse(this::addHttp2Handlers);
        this.authHandler = builder.authHandler;
        this.logger = Optional.ofNullable(builder.logger).orElseGet(this::getDefaultLogger);
        this.hsts = builder.hsts;
        this.holder = builder.holder;
    }

    private Logger getDefaultLogger() {
        return LogManager.getLogger();
    }

    public BiFunction<SSLEngine, List<String>, String> getAlpnSelector() {
        boolean canHttp2 = modelSetup != null || modelSetup2 != null;
        boolean canHttp1 = modelSetup != null;
        return (e, l) -> {
            for (String p: l) {
                if (ApplicationProtocolNames.HTTP_2.equals(p) && canHttp2) {
                    return ApplicationProtocolNames.HTTP_2;
                } else if (ApplicationProtocolNames.HTTP_1_1.equals(p) && canHttp1) {
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
        String protocol = p.channel().attr(AbstractIpTransport.ALPNPROTOCOL).get();
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            addHttp2Handlers(p);
        } else {
            addHttp1Handlers(p);
        }
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
        p.addLast(Http2FrameCodecBuilder.forServer().build());
        Http2MultiplexHandler multiplexHandler = new Http2MultiplexHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline cp = ch.pipeline();
                Channel parentChannel = ch.parent();
                for (AttributeKey<?> key : List.of(
                        NettyTransport.PRINCIPALATTRIBUTE,
                        AbstractIpTransport.SSLSENGINATTRIBUTE, AbstractIpTransport.SSLSESSIONATTRIBUTE, AbstractIpTransport.ALPNPROTOCOL,
                        HttpChannelConsumer.HOLDERATTRIBUTE, HttpChannelConsumer.STARTTIMEATTRIBUTE
                        )
                ) {
                    copyAttribue(key, parentChannel, ch);
                }
                cp.addLast(new Http2StreamFrameToHttpObjectCodec(true, false));
                finishPipelineSetup(cp);
            }
        });
        p.addLast(multiplexHandler);
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
            logger.error("Invalid pipeline configuration: {}", e::getMessage);
            logger.catching(Level.DEBUG, e);
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
        logger.atError().withThrowable(logger.getLevel().isLessSpecificThan(Level.DEBUG) ? cause : null).log("Unable to process query: {}", () -> Helpers.resolveThrowableException(cause));
        ctx.pipeline().addAfter(HTTP_OBJECT_AGGREGATOR, "FatalErrorHandler", FATAL_ERROR);
    }

    @Override
    public void logFatalException(Throwable ex) {
        logger.fatal("Caught fatal exception", ex);
    }

}
