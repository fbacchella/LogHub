package loghub.netty.http;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.servers.AbstractTcpServer;

public abstract class AbstractHttpServer<S extends AbstractHttpServer<S, B>, 
                                         B extends AbstractHttpServer.Builder<S, B>
                                        > extends AbstractTcpServer<S, B> implements ChannelConsumer<ServerBootstrap, ServerChannel> {

    public abstract static class Builder<S extends AbstractHttpServer<S, B>,
                                         B extends AbstractHttpServer.Builder<S, B>
                                        > extends AbstractTcpServer.Builder<S, B> {
        Supplier<HttpObjectAggregator> aggregatorSupplier;
        int maxContentLength = 1048576;
        protected Builder() {
        }
        @SuppressWarnings("unchecked")
        public B setAggregatorSupplier(Supplier<HttpObjectAggregator> aggregatorSupplier) {
            this.aggregatorSupplier = aggregatorSupplier;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setMaxContentLength(int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return (B) this;
        }
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> NOTFOUND = new NotFound();
    private final Supplier<HttpObjectAggregator> aggregatorSupplier;

    protected AbstractHttpServer(B builder) throws IllegalArgumentException, InterruptedException {
        super(builder);
        aggregatorSupplier = Optional.ofNullable(builder.aggregatorSupplier).orElseGet(() -> () -> new HttpObjectAggregator(builder.maxContentLength));
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", new HttpServerCodec());
        p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
        p.addLast("HttpContentCompressor", new HttpContentCompressor(9, 15, 8));
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        p.addLast("HttpObjectAggregator", aggregatorSupplier.get());
        try {
            addModelHandlers(p);
        } catch (ChannelPipelineException e) {
            logger.error("Invalid pipeline configuration: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            p.addAfter("HttpObjectAggregator", "BrokenConfigHandler", getFatalErrorHandler());
        }
        p.addLast("NotFound404", NOTFOUND);
        super.addHandlers(p);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unable to process query: {}", Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
        ctx.pipeline().addAfter("HttpObjectAggregator", "FatalErrorHandler", getFatalErrorHandler());
    }

    private HttpRequestProcessing getFatalErrorHandler() {
        return new HttpRequestProcessing() {
            @Override
            public boolean acceptRequest(HttpRequest request) {
                return true;
            }
            @Override
            protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
                throw new HttpRequestFailure(HttpResponseStatus.SERVICE_UNAVAILABLE, "Unable to process request because of invalid configuration");
            }
            @Override
            protected String getContentType(HttpRequest request, HttpResponse response) {
                return null;
            }
        };
    }

    public abstract void addModelHandlers(ChannelPipeline p);

    @Override
    public void addOptions(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

}
