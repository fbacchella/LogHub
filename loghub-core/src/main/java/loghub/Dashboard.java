package loghub;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.security.auth.login.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.GetMetric;
import loghub.netty.http.GraphMetric;
import loghub.netty.http.HstsData;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.JwtToken;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;
import loghub.netty.http.TokenFilter;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TcpTransport;
import loghub.security.AuthenticationHandler;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Dashboard {

    private static final Logger logger = LogManager.getLogger();

    @Setter
    @Accessors(chain=true)
    public static class Builder {
        int port = -1;
        String listen = null;
        boolean withSSL = false;
        SSLContext sslContext = null;
        Duration hstsDuration = null;
        SSLParameters sslParams = null;
        ClientAuthentication sslClientAuthentication = ClientAuthentication.NONE;
        String sslKeyAlias = null;
        POLLER poller = POLLER.DEFAULTPOLLER;
        Configuration jaasConfigJwt = null;
        String jaasNameJwt = null;
        boolean withJwtUrl = false;
        JWTHandler jwtHandlerUrl = null;
        boolean withJolokia = false;
        String jolokiaPolicyLocation = null;
        ClassLoader classLoader = Dashboard.class.getClassLoader();
        public Dashboard build() {
            return new Dashboard(this);
        }
    }

    public static Dashboard.Builder getBuilder() {
        return new Dashboard.Builder();
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();
    private final SimpleChannelInboundHandler<FullHttpRequest> JOLOKIA_SERVICE;
    private final SimpleChannelInboundHandler<FullHttpRequest> GETMETRIC = new GetMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> GRAPHMETRIC = new GraphMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenGenerator;
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenFilter;
    @Getter
    private final TcpTransport transport;

    private Dashboard(Builder builder) {
        AuthenticationHandler authHandler = getAuthenticationHandler(builder.withJwtUrl, builder.jwtHandlerUrl,
                                                                     builder.jaasNameJwt, builder.jaasConfigJwt);
        HstsData hsts = null;
        if (builder.withSSL && builder.hstsDuration != null) {
            hsts = HstsData.builder().maxAge(builder.hstsDuration).build();
        }
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                                          .setAuthHandler(authHandler)
                                                          .setModelSetup(this::setupModel)
                                                          .setLogger(logger)
                                                          .setHsts(hsts)
                                                          .build();

        transport = getTransport(builder, consumer);
        if (authHandler != null && authHandler.getJwtHandler() != null) {
            tokenGenerator = new JwtToken(authHandler.getJwtHandler());
            tokenFilter = new TokenFilter(authHandler);
        } else {
            tokenGenerator = null;
            tokenFilter = null;
        }
        if (builder.withJolokia) {
            logger.debug("Jolokia requested");
            // temporary variable as the stacking of exceptions confuse the compiler
            SimpleChannelInboundHandler<FullHttpRequest> jolokiaServiceTemp = null;
            try {
                Class<?> jsClass = builder.classLoader.loadClass("loghub.netty.http.JolokiaService");
                Map<String, Object> jsProps;
                if (builder.jolokiaPolicyLocation != null) {
                    jsProps = Map.of("jolokiaPolicyLocation", builder.jolokiaPolicyLocation);
                } else {
                    jsProps = Map.of();
                }
                Method ofMethod = jsClass.getMethod("of", Map.class);
                jolokiaServiceTemp = (SimpleChannelInboundHandler<FullHttpRequest>)ofMethod.invoke(null, jsProps);
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
                logger.atError().withThrowable(ex).log("Failed to start Jolokia service: {}", () -> Helpers.resolveThrowableException(ex));
            }
            JOLOKIA_SERVICE = jolokiaServiceTemp;
        } else {
            JOLOKIA_SERVICE = null;
        }
    }

    private TcpTransport getTransport(Builder builder, HttpChannelConsumer consumer) {
        TcpTransport.Builder transportBuilder = TRANSPORT.TCP.getBuilder();
        transportBuilder.setThreadPrefix("Dashboard");
        transportBuilder.setPoller(builder.poller);
        transportBuilder.setConsumer(consumer);
        transportBuilder.setEndpoint(builder.listen);
        transportBuilder.setPort(builder.port);

        if (builder.withSSL) {
            transportBuilder.setWithSsl(true);
            transportBuilder.setSslContext(builder.sslContext);
            transportBuilder.setSslParams(builder.sslParams);
            transportBuilder.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
            transportBuilder.setSslKeyAlias(builder.sslKeyAlias);
            transportBuilder.setSslClientAuthentication(builder.sslClientAuthentication);
        }
        return transportBuilder.build();
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
        if (JOLOKIA_SERVICE != null) {
            p.addLast(JOLOKIA_SERVICE);
        }
        p.addLast(GETMETRIC);
        p.addLast(GRAPHMETRIC);
        if (tokenGenerator != null && tokenFilter != null) {
            p.addLast(tokenFilter);
            p.addLast(tokenGenerator);
        }
    }

    public void start() throws InterruptedException {
        logger.debug("Starting the dashboard");
        transport.bind();
    }

    public void stop() {
        transport.close();
    }

}
