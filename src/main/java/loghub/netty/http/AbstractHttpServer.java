package loghub.netty.http;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.servers.NettyServer;
import loghub.security.AuthenticationHandler;

public abstract class AbstractHttpServer<B extends AbstractHttpServer.Builder>
        implements ChannelConsumer {

    public abstract static class Builder<S extends AbstractHttpServer,
                                         B extends AbstractHttpServer.Builder<S,B>
                                        > extends NettyServer.Builder<S,B> {
        Supplier<HttpObjectAggregator> aggregatorSupplier;
        Supplier<HttpServerCodec> serverCodecSupplier;
        int maxContentLength = 1048576;
        AuthenticationHandler authHandler = null;
        protected Builder() {
        }
        @SuppressWarnings("unchecked")
        public B setAggregatorSupplier(Supplier<HttpObjectAggregator> aggregatorSupplier) {
            this.aggregatorSupplier = aggregatorSupplier;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setServerCodecSupplier(Supplier<HttpServerCodec> serverCodecSupplier) {
            this.serverCodecSupplier = serverCodecSupplier;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setMaxContentLength(int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return (B) this;
        }
    }

    private static final SimpleChannelInboundHandler<FullHttpRequest> NOT_FOUND = new NotFound();
    private static final SimpleChannelInboundHandler<FullHttpRequest> FATAL_ERROR = new FatalErrorHandler();

    private final Supplier<HttpObjectAggregator> aggregatorSupplier;
    private final Supplier<HttpServerCodec> serverCodecSupplier;
    protected final Logger logger;

    protected AbstractHttpServer(B builder) {
        super(builder);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElse(() -> new HttpObjectAggregator(builder.maxContentLength));
        serverCodecSupplier = Optional.ofNullable(builder.serverCodecSupplier).orElse(HttpServerCodec::new);
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", serverCodecSupplier.get());
        p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
        p.addLast("HttpContentCompressor", new HttpContentCompressor());
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        p.addLast("HttpObjectAggregator", aggregatorSupplier.get());
        if (authHandler != null) {
            p.addLast("Authentication", new AccessControl(authHandler));
            logger.debug("Added authentication");
        }
        try {
            addModelHandlers(p);
        } catch (ChannelPipelineException e) {
            logger.error("Invalid pipeline configuration: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            p.addAfter("HttpObjectAggregator", "BrokenConfigHandler", FATAL_ERROR);
        }
        p.addLast("NotFound404", NOT_FOUND);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unable to process query: {}", Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
        ctx.pipeline().addAfter("HttpObjectAggregator", "FatalErrorHandler", FATAL_ERROR);
    }

    @Override
    public void logFatalException(Throwable ex) {
        logger.fatal("Caught fatal exception", ex);
    }

    public abstract void addModelHandlers(ChannelPipeline p);

}
