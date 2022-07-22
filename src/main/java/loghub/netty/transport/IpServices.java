package loghub.netty.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Collections;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
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

    // A class is need to protected SUPPORTED_APN
    class ConstantHolder {
        private ConstantHolder() { }
        private static final String[] SUPPORTED_APN = new String[]{ApplicationProtocolNames.HTTP_1_1};
    }
    AttributeKey<SSLSession> SSLSESSIONATTRIBUTE = AttributeKey.newInstance(SSLSession.class.getName());
    AttributeKey<SSLEngine> SSLSENGINATTRIBUTE = AttributeKey.newInstance(SSLEngine.class.getName());


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
        if (config.rcvBuf > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, config.rcvBuf);
        }
        if (config.sndBuf > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, config.sndBuf);
        }
    }

    default void addSslHandler(TransportConfig config, ChannelPipeline pipeline, Logger logger) {
        logger.debug("adding an SSL handler on {}", pipeline::channel);
        SSLEngine engine = getEngine(config);
        engine.setUseClientMode(false);
        SslHandler sslHandler = new SslHandler(engine);
        pipeline.addFirst("ssl", sslHandler);
        Future<Channel> future = sslHandler.handshakeFuture();
        future.addListener((GenericFutureListener<Future<Channel>>) f -> {
            SSLSession sess = sslHandler.engine().getSession();
            logger.trace("SSL started with {}", () -> sess);
            Principal principal = checkSslClient(config.sslClientAuthentication, sess, logger);
            logger.debug("Got SSL client identity '{}'", () -> (principal != null ? principal.getName() : ""));
            if (principal != null) {
                f.get().attr(PRINCIPALATTRIBUTE).set(principal);
            }
            f.get().attr(SSLSESSIONATTRIBUTE).set(sess);
            f.get().attr(SSLSENGINATTRIBUTE).set(engine);
        });
    }

    default void addSslClientHandler(TransportConfig config, ChannelPipeline pipeline, Logger logger) {
        logger.debug("adding an SSL client handler on {}", pipeline::channel);
        SSLEngine engine = getEngine(config);
        SSLParameters params = engine.getSSLParameters();
        params.setServerNames(Collections.singletonList(new SNIHostName(config.endpoint)));
        params.setApplicationProtocols(ConstantHolder.SUPPORTED_APN);
        engine.setSSLParameters(params);
        engine.setUseClientMode(true);
        pipeline.addLast("ssl", new SslHandler(engine));
    }

    private static SSLEngine getEngine(TransportConfig config) {
        SSLEngine engine;
        if (config.sslContext != null) {
            if (config.sslKeyAlias != null && ! config.sslKeyAlias.isEmpty()) {
                engine = config.sslContext.createSSLEngine(config.sslKeyAlias, DEFINEDSSLALIAS);
            } else {
                engine = config.sslContext.createSSLEngine();
            }
        } else {
            try {
                engine = SSLContext.getDefault().createSSLEngine();
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalStateException("SSL context invalid", ex);
            }
        }

        SSLParameters params = engine.getSSLParameters();
        params.setUseCipherSuitesOrder(true);
        params.setApplicationProtocols(new String[]{ApplicationProtocolNames.HTTP_1_1});
        engine.setSSLParameters(params);

        if (config.sslClientAuthentication != null) {
            config.sslClientAuthentication.configureEngine(engine);
        }
        return engine;
    }

    private static Principal checkSslClient(ClientAuthentication sslClientAuthentication, SSLSession sess, Logger logger) throws GeneralSecurityException {
        logger.debug("Testing ssl client authentication");
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
