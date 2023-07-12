package loghub.netty.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractIpTransport<M, T extends AbstractIpTransport<M, T, B>, B extends AbstractIpTransport.Builder<M, T, B>> extends  NettyTransport <InetSocketAddress, M, T, B> {

    public static final AttributeKey<SSLSession> SSLSESSIONATTRIBUTE = AttributeKey.newInstance(SSLSession.class.getName());
    public static final AttributeKey<SSLEngine> SSLSENGINATTRIBUTE = AttributeKey.newInstance(SSLEngine.class.getName());
    public static final int DEFINEDSSLALIAS=-2;

    public abstract static class Builder<M, T extends AbstractIpTransport<M,T, B>, B extends AbstractIpTransport.Builder<M, T, B>> extends NettyTransport.Builder<InetSocketAddress, M, T, B> {
        @Setter
        protected int port;
        @Setter
        protected int rcvBuf;
        @Setter
        protected int sndBuf;
        @Setter
        protected boolean withSsl;
        @Setter
        protected SSLContext sslContext;
        @Setter
        protected String sslKeyAlias;
        @Setter
        protected ClientAuthentication sslClientAuthentication;
        protected List<String> applicationProtocols = new ArrayList<>(0);
        public void addApplicationProtocol(String protocol) {
            applicationProtocols.add(protocol);
        }
    }

    @Getter
    protected final int port;
    @Getter
    protected final int rcvBuf;
    @Getter
    protected final int sndBuf;
    @Getter
    protected final boolean withSsl;
    @Getter
    protected final SSLContext sslContext;
    @Getter
    protected final String sslKeyAlias;
    @Getter
    protected final ClientAuthentication sslClientAuthentication;
    @Getter
    protected final List<String> applicationProtocols;

    protected AbstractIpTransport(B builder) {
        super(builder);
        this.port = builder.port;
        this.rcvBuf = builder.rcvBuf;
        this.sndBuf = builder.sndBuf;
        this.withSsl = builder.withSsl;
        this.sslContext = builder.sslContext;
        this.sslKeyAlias = builder.sslKeyAlias;
        this.sslClientAuthentication = builder.sslClientAuthentication;
        this.applicationProtocols = Collections.unmodifiableList(builder.applicationProtocols);
    }

    public InetSocketAddress resolveAddress() {
        try {
            return new InetSocketAddress((endpoint != null && ! endpoint.isBlank() && ! "*".equals(endpoint)) ? InetAddress.getByName(endpoint) : null, port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Unknown host to bind: " + endpoint + ":" + port);
        }
    }

    void configureAbstractBootStrap(AbstractBootstrap<?, ?> bootstrap) {
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        if (rcvBuf > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, rcvBuf);
        }
        if (sndBuf > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, sndBuf);
        }
    }

    @Override
    protected void initChannel(Channel ch, boolean client) {
        super.initChannel(ch, client);
        if (withSsl && client) {
            addSslClientHandler(ch.pipeline());
        } else if (withSsl) {
            addSslHandler(ch.pipeline());
        }
    }

    void addSslHandler(ChannelPipeline pipeline) {
        logger.debug("Adding an SSL handler on {}", pipeline::channel);
        SSLEngine engine = getEngine();
        engine.setUseClientMode(false);
        SslHandler sslHandler = new SslHandler(engine);
        pipeline.addFirst( "ssl", sslHandler);
        Future<Channel> future = sslHandler.handshakeFuture();
        future.addListener((GenericFutureListener<Future<Channel>>) f -> {
            SSLSession sess = sslHandler.engine().getSession();
            logger.trace("SSL started with {}", () -> sess);
            Principal principal = checkSslClient(sslClientAuthentication, sess, logger);
            logger.debug("Got SSL client identity '{}'", () -> (principal != null ? principal.getName() : ""));
            if (principal != null) {
                f.get().attr(PRINCIPALATTRIBUTE).set(principal);
            }
            f.get().attr(SSLSESSIONATTRIBUTE).set(sess);
            f.get().attr(SSLSENGINATTRIBUTE).set(engine);
        });
    }

    void addSslClientHandler(ChannelPipeline pipeline) {
        logger.debug("Adding an SSL client handler on {}", pipeline::channel);
        SSLEngine engine = getEngine();
        SSLParameters params = engine.getSSLParameters();
        params.setServerNames(Collections.singletonList(new SNIHostName(endpoint)));
        params.setApplicationProtocols(applicationProtocols.toArray(String[]::new));
        engine.setSSLParameters(params);
        engine.setUseClientMode(true);
        pipeline.addLast("ssl", new SslHandler(engine));
    }

    private SSLEngine getEngine() {
        SSLEngine engine;
        if (sslContext != null) {
            if (sslKeyAlias != null && ! sslKeyAlias.isEmpty()) {
                engine = sslContext.createSSLEngine(sslKeyAlias, DEFINEDSSLALIAS);
            } else {
                engine = sslContext.createSSLEngine();
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

        if (sslClientAuthentication != null) {
            sslClientAuthentication.configureEngine(engine);
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
