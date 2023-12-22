package loghub.netty;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import loghub.Helpers;
import loghub.netty.http.AccessControl;
import loghub.netty.http.FatalErrorHandler;
import loghub.netty.http.NotFound;
import loghub.security.AuthenticationHandler;
import lombok.Setter;
import lombok.experimental.Accessors;

public class HttpChannelConsumer implements ChannelConsumer {

    private static final SimpleChannelInboundHandler<FullHttpRequest> NOT_FOUND = new NotFound();
    private static final SimpleChannelInboundHandler<FullHttpRequest> FATAL_ERROR = new FatalErrorHandler();
    private static final String HTTP_OBJECT_AGGREGATOR = "HttpObjectAggregator";

    @Accessors(chain = true)
    public static class Builder {
        // Both aggregatorSupplier and serverCodecSupplier needs a supplier because
        // they are usually not sharable, so each pipeline needs its own instance.
        @Setter
        private Supplier<HttpObjectAggregator> aggregatorSupplier;
        @Setter
        private Supplier<HttpServerCodec> serverCodecSupplier;
        @Setter
        private Consumer<ChannelPipeline> modelSetup;
        @Setter
        private AuthenticationHandler authHandler;
        @Setter
        private int maxContentLength = 1048576;
        @Setter
        private Logger logger;
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

    protected HttpChannelConsumer(Builder builder) {
        this.aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElse(() -> new HttpObjectAggregator(builder.maxContentLength));
        this.serverCodecSupplier = Optional.ofNullable(builder.serverCodecSupplier).orElse(HttpServerCodec::new);
        this.modelSetup = builder.modelSetup;
        this.authHandler = builder.authHandler;
        this.logger = Optional.ofNullable(builder.logger).orElseGet(LogManager::getLogger);
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", serverCodecSupplier.get());
        p.addLast("HttpContentDeCompressor", new HttpContentDecompressor());
        p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
        p.addLast("HttpContentCompressor", new HttpContentCompressor());
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        p.addLast(HTTP_OBJECT_AGGREGATOR, aggregatorSupplier.get());
        if (authHandler != null) {
            p.addLast("Authentication", new AccessControl(authHandler));
            logger.debug("Added authentication");
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
        logger.error("Unable to process query: {}", () -> Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
        ctx.pipeline().addAfter(HTTP_OBJECT_AGGREGATOR, "FatalErrorHandler", FATAL_ERROR);
    }

    @Override
    public void logFatalException(Throwable ex) {
        logger.fatal("Caught fatal exception", ex);
    }

}
