package loghub.netty.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import javax.net.ssl.SSLContext;

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
import loghub.netty.servers.TcpServer;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;

public abstract class AbstractHttpServer extends TcpServer implements ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> {

    public abstract static class Builder<S extends AbstractHttpServer> {
        private int port = -1;
        private String host = null;
        private SSLContext sslctx = null;
        private AuthenticationHandler authHandler = null;
        private boolean useSSL = false;
        private ClientAuthentication sslClientAuthentication = ClientAuthentication.NONE;
        private String alias = null;
        protected Builder() {
        }

        public Builder<S> setHost(String host) {
            this.host = host != null && !host.isEmpty() ? host : null;
            return this;
        }
        public Builder<S> setPort(int port) {
            this.port = port;
            return this;
        }
        public Builder<S> setSSLContext(SSLContext sslctx) {
            this.sslctx = sslctx;
            return this;
        }
        public Builder<S> useSSL(boolean useSSL) {
            this.useSSL = useSSL;
            return this;
        }
        public Builder<S> setSSLClientAuthentication(ClientAuthentication sslClientAuthentication) {
            this.sslClientAuthentication = sslClientAuthentication;
            return this;
        }
        public Builder<S> setAuthHandler(AuthenticationHandler authHandler) {
            this.authHandler = authHandler;
            return this;
        }
        public Builder<S> setSSLKeyAlias(String alias) {
            this.alias = alias;
            return this;
        }
        public abstract S build();
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> NOTFOUND = new NotFound();

    private final int port;
    private final String host;
    private final AuthenticationHandler authHandler;

    protected AbstractHttpServer(Builder<?> builder) {
        port = builder.port;
        host = builder.host;
        authHandler = builder.authHandler;
        if (builder.useSSL) {
            setSSLContext(builder.sslctx);
            setSSLClientAuthentication(builder.sslClientAuthentication);
            setSSLKeyAlias(builder.alias );
        }
    }

    @Override
    public boolean configure(ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> consumer) {
        setThreadFactory(new DefaultThreadFactory("builtinhttpserver/" +
                consumer.getListenAddress().getAddress().getHostAddress() + ":" + consumer.getListenAddress().getPort()));
        return super.configure(consumer);
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
        if (isWithSSL()) {
            addSslHandler(p, getEngine());
        }
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

    @Override
    public InetSocketAddress getListenAddress() {
        try {
            return new InetSocketAddress(host != null ? InetAddress.getByName(host) : null , port);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return null;
        }
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    public AuthenticationHandler getAuthHandler() {
        return authHandler;
    }

}
