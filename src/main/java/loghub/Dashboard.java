package loghub;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.GetMetric;
import loghub.netty.http.GraphMetric;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.JwtToken;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;
import loghub.netty.http.TokenFilter;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TransportConfig;
import loghub.security.AuthenticationHandler;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Dashboard {

    private static final Logger logger = LogManager.getLogger();

    @Accessors(chain=true)
    public static class Builder {
        @Setter
        int port = -1;
        @Setter
        String listen = null;
        @Setter
        boolean withSSL = false;
        @Setter
        SSLContext sslContext = null;
        @Setter
        ClientAuthentication sslClientAuthentication = ClientAuthentication.NONE;
        @Setter
        String sslKeyAlias = null;
        @Setter
        POLLER poller = POLLER.DEFAULTPOLLER;
        @Setter
        Configuration jaasConfigJwt = null;
        @Setter
        String jaasNameJwt = null;
        @Setter
        boolean withJwtUrl = false;
        @Setter
        JWTHandler jwtHandlerUrl = null;
        public Dashboard build() {
            return new Dashboard(this);
        }
    }

    public static Dashboard.Builder getBuilder() {
        return new Dashboard.Builder();
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();
    private final SimpleChannelInboundHandler<FullHttpRequest> GETMETRIC = new GetMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> GRAPHMETRIC = new GraphMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenGenerator;
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenFilter;
    private final TransportConfig config;
    private final NettyTransport<InetSocketAddress, ByteBuf> transport;

    private Dashboard(Builder builder) {
        AuthenticationHandler authHandler = getAuthenticationHandler(builder.withJwtUrl, builder.jwtHandlerUrl,
                                                                     builder.jaasNameJwt, builder.jaasConfigJwt);
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                      .setAuthHandler(authHandler)
                                      .setModelSetup(this::setupModel)
                                      .build();
        transport = TRANSPORT.TCP.getInstance(builder.poller);
        config = new TransportConfig();
        config.setConsumer(consumer);
        config.setThreadPrefix("Dashboard");
        config.setEndpoint(builder.listen);
        config.setPort(builder.port);
        if (builder.withSSL) {
            config.setWithSsl(true).setSslContext(builder.sslContext).setSslKeyAlias(builder.sslKeyAlias).setSslClientAuthentication(builder.sslClientAuthentication);
        }
        if (authHandler != null && authHandler.getJwtHandler() != null) {
            tokenGenerator = new JwtToken(authHandler.getJwtHandler());
            tokenFilter = new TokenFilter(authHandler);
        } else {
            tokenGenerator = null;
            tokenFilter = null;
        }
    }

    private AuthenticationHandler getAuthenticationHandler(boolean withJwt, JWTHandler jwtHandler,
            String jaasName, Configuration jaasConfig) {
        AuthenticationHandler.Builder authHandlerBuilder = AuthenticationHandler.getBuilder();
        if (withJwt) {
            authHandlerBuilder.useJwt(true).setJwtHandler(jwtHandler);
        } else {
            authHandlerBuilder.useJwt(false);
        }
        if (jaasName != null && ! jaasName.isBlank()) {
            authHandlerBuilder.setJaasName(jaasName).setJaasConfig(jaasConfig);
        }
        return authHandlerBuilder.build();
    }

    private void setupModel(ChannelPipeline p) {
        p.addLast(ROOTREDIRECT);
        p.addLast(new ResourceFiles());
        p.addLast(JMXPROXY);
        p.addLast(GETMETRIC);
        p.addLast(GRAPHMETRIC);
        if (tokenGenerator != null && tokenFilter != null) {
            p.addLast(tokenFilter);
            p.addLast(tokenGenerator);
        }
    }

    public void start() throws InterruptedException {
        logger.debug("Starting an HTTP server with configuration {}", config);
        transport.bind(config);
    }

    public void stop() {
        transport.close();
    }

}
