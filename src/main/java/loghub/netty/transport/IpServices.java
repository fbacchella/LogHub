package loghub.netty.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.FailedLoginException;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;
import loghub.security.ssl.ClientAuthentication;

import static loghub.netty.transport.NettyTransport.DEFINEDSSLALIAS;
import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

public interface IpServices {

    public static final AttributeKey<SSLSession> SSLSESSIONATTRIBUTE = AttributeKey.newInstance(SSLSession.class.getName());
    public static final AttributeKey<SSLEngine> SSLSENGINATTRIBUTE = AttributeKey.newInstance(SSLEngine.class.getName());

    default InetSocketAddress resolveAddress(TransportConfig config) {
        try {
            return new InetSocketAddress((config.endpoint != null && ! config.endpoint.isBlank() && ! "*".equals(config.endpoint)) ? InetAddress.getByName(config.endpoint) : null, config.port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Unknow host to bind: " + config.endpoint + ":" + config.port);
        }
    }

    default ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        SocketAddress remoteChannelAddr = ctx.channel().remoteAddress();
        SocketAddress localChannelAddr = ctx.channel().localAddress();
        InetSocketAddress remoteaddr = null;
        InetSocketAddress localaddr = null;
        if (remoteChannelAddr instanceof InetSocketAddress) {
            remoteaddr = (InetSocketAddress)remoteChannelAddr;
        }
        if (localChannelAddr instanceof InetSocketAddress) {
            localaddr = (InetSocketAddress)localChannelAddr;
        }

        Attribute<SSLSession> sess = ctx.channel().attr(SSLSESSIONATTRIBUTE);
        return new IpConnectionContext(localaddr, remoteaddr, sess.get());
    }

    default void configureAbstractBootStrap(AbstractBootstrap<?, ?> bootstrap, TransportConfig config) {
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        if (config.rcvBuf >= 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, config.rcvBuf);
        }
        if (config.sndBuf >= 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, config.sndBuf);
        }
    }

    default void addSslHandler(TransportConfig config, ChannelPipeline pipeline, Logger logger) {
        logger.debug("adding an SSL handler on {}", pipeline::channel);
        SSLEngine engine = getEngine(config);
        SslHandler sslHandler = new SslHandler(engine);
        pipeline.addFirst("ssl", sslHandler);
        Future<Channel> future = sslHandler.handshakeFuture();
        future.addListener((GenericFutureListener<Future<Channel>>) f -> {
            SSLSession sess = sslHandler.engine().getSession();
            logger.trace("SSL started with {}", () -> sess);
            Principal principal = checkSslClient(config.sslClientAuthentication, sess, logger);
            logger.debug("Got SSL client identity {}", () -> (principal != null ? principal.getName() : ""));
            if (principal != null) {
                f.get().attr(PRINCIPALATTRIBUTE).set(principal);
            }
            f.get().attr(SSLSESSIONATTRIBUTE).set(sess);
            f.get().attr(SSLSENGINATTRIBUTE).set(engine);
        });
    }

    private static SSLEngine getEngine(TransportConfig config) {
        SSLEngine engine;
        if (config.sslKeyAlias != null && ! config.sslKeyAlias.isEmpty()) {
            engine = config.sslctx.createSSLEngine(config.sslKeyAlias, DEFINEDSSLALIAS);
        } else {
            engine = config.sslctx.createSSLEngine();
        }

        SSLParameters params = engine.getSSLParameters();
        String[] protocols = Arrays.stream(params.getProtocols()).filter(s -> ! s.startsWith("SSL")).filter(s -> ! "TLSv1".equals(s)).filter(s -> ! "TLSv1.1".equals(s)).toArray(String[]::new);
        params.setProtocols(protocols);
        String[] cipherSuites = Arrays.stream(params.getCipherSuites()).filter(s -> ! s.contains("CBC")).toArray(String[]::new);
        params.setCipherSuites(cipherSuites);
        params.setUseCipherSuitesOrder(true);
        params.setApplicationProtocols(new String[]{ApplicationProtocolNames.HTTP_1_1});
        engine.setSSLParameters(params);

        engine.setUseClientMode(false);
        config.sslClientAuthentication.configureEngine(engine);
        return engine;
    }

    private static Principal checkSslClient(ClientAuthentication sslClientAuthentication, SSLSession sess, Logger logger) throws GeneralSecurityException {
        logger.debug("testing ssl client authentication");
        if (sslClientAuthentication != ClientAuthentication.NOTNEEDED) {
            try {
                if (sslClientAuthentication == ClientAuthentication.WANTED || sslClientAuthentication == ClientAuthentication.REQUIRED) {
                    return sess.getPeerPrincipal();
                }
            } catch (SSLPeerUnverifiedException e) {
                if (sslClientAuthentication == ClientAuthentication.REQUIRED) {
                    throw new FailedLoginException("Client authentication required but failed");
                } else {
                    return null;
                }
            }
        }
        return null;
    }

}
