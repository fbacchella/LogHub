package loghub.netty.servers;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import loghub.Helpers;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TransportConfig;
import loghub.security.AuthenticationHandler;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class HttpServer<S extends HttpServer> {

    private class ConsumerBuilder extends HttpChannelConsumer.Builder<HttpChannelConsumer> {
        @Override
        protected HttpChannelConsumer build() {
            return new HttpServerConsumer(this);
        }
    }

    private class HttpServerConsumer extends HttpChannelConsumer<HttpServerConsumer> {
        public HttpServerConsumer(Builder builder) {
            super(builder);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            HttpServer.this.addModelHandlers(p);
        }
    }

    @Accessors(chain=true)
    public abstract static class Builder<S extends HttpServer> {
        @Setter
        int port = -1;
        @Setter
        String listen = null;
        @Setter
        boolean withSSL = false;
        @Setter
        SSLContext sslContext = null;
        @Setter
        ClientAuthentication sslClientAuthentication = null;
        @Setter
        String sslKeyAlias = null;
        @Setter
        Configuration jaasConfig = null;
        @Setter
        String jaasName = null;
        @Setter
        boolean withJwt = false;
        @Setter
        JWTHandler jwtHandler = null;
        @Setter
        POLLER poller = POLLER.DEFAULTPOLLER;
        public abstract S build();
    }

    protected final TransportConfig config;
    protected final NettyTransport<InetSocketAddress, ByteBuf> transport;
    protected final Logger logger;

    protected HttpServer(Builder<S> builder) {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        AuthenticationHandler.Builder authHandlerBuilder = AuthenticationHandler.getBuilder();
        if (builder.withJwt) {
            authHandlerBuilder.useJwt(builder.withJwt).setJwtHandler(builder.jwtHandler);
        } else {
            authHandlerBuilder.useJwt(false);
        }
        if (builder.jaasName != null && ! builder.jaasName.isBlank()) {
            authHandlerBuilder.setJaasName(builder.jaasName).setJaasConfig(builder.jaasConfig);
        }
        AuthenticationHandler authHandler = authHandlerBuilder.build();
        ConsumerBuilder consumerbuilder = new ConsumerBuilder();
        consumerbuilder.setAuthHandler(authHandler);
        transport = TRANSPORT.TCP.getInstance(builder.poller);
        config = new TransportConfig();
        config.setConsumer(consumerbuilder.build());
        config.setThreadPrefix("HttpServer");
        config.setEndpoint(builder.listen);
        config.setPort(builder.port);
        if (builder.withSSL) {
            config.setSslContext(builder.sslContext).setSslKeyAlias(builder.sslKeyAlias).setSslClientAuthentication(builder.sslClientAuthentication);
        }
    }

    protected abstract void addModelHandlers(ChannelPipeline p);

    public void start() throws ExecutionException, InterruptedException {
        logger.debug("Starting an HTTP server with configuration {}", config);
        transport.bind(config);
    }

    public void stop() {
        transport.close();
    }

}
