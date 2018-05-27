package loghub.netty.http;

import java.net.InetSocketAddress;

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
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.servers.AbstractTcpServer;

public abstract class AbstractHttpServer<S extends AbstractHttpServer<S, B>, 
                                         B extends AbstractHttpServer.Builder<S, B>
                                        > extends AbstractTcpServer<S, B> implements ChannelConsumer<ServerBootstrap, ServerChannel> {

    public abstract static class Builder<S extends AbstractHttpServer<S, B>,
                                         B extends AbstractHttpServer.Builder<S, B>
                                        > extends AbstractTcpServer.Builder<S, B> {
        protected Builder() {
        }
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> NOTFOUND = new NotFound();

    protected AbstractHttpServer(B builder) {
        super(builder);
    }

    @Override
    public boolean configure(ChannelConsumer<ServerBootstrap, ServerChannel> consumer) {
        InetSocketAddress address = getAddress();
        setThreadFactory(new DefaultThreadFactory("builtinhttpserver/" +
                        address.getAddress().getHostAddress() + ":" + address.getPort()));
        return super.configure(consumer);
    }

    public boolean configure() {
        return configure(this);
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", new HttpServerCodec());
        p.addLast("HttpContentCompressor", new HttpContentCompressor(9, 15, 8));
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        p.addLast("HttpObjectAggregator", new HttpObjectAggregator(1048576));
        try {
            addModelHandlers(p);
        } catch (ChannelPipelineException e) {
            logger.error("Invalid pipeline configuration: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            p.addAfter("HttpObjectAggregator", "BrokenConfigHandler", getFatalErrorHandler());
        }
        p.addLast(NOTFOUND);
        super.addHandlers(p);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unable to process query: {}", Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
        ctx.pipeline().addFirst(getFatalErrorHandler());
    }

    private HttpRequestProcessing getFatalErrorHandler() {
        return new HttpRequestProcessing() {
            @Override
            public boolean acceptRequest(HttpRequest request) {
                return true;
            }
            @Override
            protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
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
