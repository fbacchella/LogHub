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
import java.util.Optional;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.FailedLoginException;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class AbstractIpTransport<M, T extends AbstractIpTransport<M, T, B>, B extends AbstractIpTransport.Builder<M, T, B>> extends NettyTransport <InetSocketAddress, M, T, B> {

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
        protected SSLParameters sslParams;
        @Setter
        protected String sslKeyAlias;
        @Setter
        protected ClientAuthentication sslClientAuthentication;
        protected final List<String> applicationProtocols = new ArrayList<>(0);
        public void addApplicationProtocol(String protocol) {
            applicationProtocols.add(protocol);
        }
    }

    protected final int port;
    protected final int rcvBuf;
    protected final int sndBuf;
    protected final boolean withSsl;
    protected final SSLContext sslContext;
    protected final SSLParameters sslParams;
    protected final String sslKeyAlias;
    protected final ClientAuthentication sslClientAuthentication;

    protected AbstractIpTransport(B builder) {
        super(builder);
        this.port = builder.port;
        this.rcvBuf = builder.rcvBuf;
        this.sndBuf = builder.sndBuf;
        this.withSsl = builder.withSsl;
        this.sslContext = Optional.ofNullable(builder.sslContext).orElseGet(this::getDefaultSslContext);
        this.sslParams = Optional.ofNullable(builder.sslParams).orElseGet(sslContext::getDefaultSSLParameters);
        this.sslKeyAlias = builder.sslKeyAlias;
        this.sslClientAuthentication = builder.sslClientAuthentication;
        sslParams.setUseCipherSuitesOrder(true);
        if (endpoint != null) {
            sslParams.setServerNames(Collections.singletonList(new SNIHostName(endpoint)));
        }
        if (! builder.applicationProtocols.isEmpty()) {
            sslParams.setApplicationProtocols(builder.applicationProtocols.toArray(String[]::new));
        }
    }

    private SSLContext getDefaultSslContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SSL context invalid", ex);
        }
    }

    public InetSocketAddress resolveAddress() {
        try {
            return new InetSocketAddress((endpoint != null && ! endpoint.isBlank() && ! "*".equals(endpoint)) ? InetAddress.getByName(endpoint) : null, port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Unknown host to bind: " + endpoint + ":" + port);
        }
    }

    void configureAbstractBootStrap(AbstractBootstrap<?, ?> bootstrap) {
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
        /* From linux man page:
            SO_RCVBUF
              Sets or gets the maximum socket receive buffer in bytes.
              The kernel doubles this value (to allow space for
              bookkeeping overhead) when it is set using setsockopt(2),
              and this doubled value is returned by getsockopt(2).  The
              default value is set by the
              /proc/sys/net/core/rmem_default file, and the maximum
              allowed value is set by the /proc/sys/net/core/rmem_max
              file.  The minimum (doubled) value for this option is 256.
        */
        if (rcvBuf > 0 && ch.config().getOption(ChannelOption.SO_RCVBUF) != (rcvBuf * 2)) {
            logger.warn("Wrong rcvBuf value, {} instead of {}", () -> ch.config().getOption(ChannelOption.SO_RCVBUF) / 2, () -> rcvBuf);
        }
        /* From linux man page:
           SO_SNDBUF
              Sets or gets the maximum socket send buffer in bytes.  The
              kernel doubles this value (to allow space for bookkeeping
              overhead) when it is set using setsockopt(2), and this
              doubled value is returned by getsockopt(2).  The default
              value is set by the /proc/sys/net/core/wmem_default file
              and the maximum allowed value is set by the
              /proc/sys/net/core/wmem_max file.  The minimum (doubled)
              value for this option is 2048.
        */
        if (sndBuf > 0 && ch.config().getOption(ChannelOption.SO_SNDBUF) != (sndBuf * 2)) {
            logger.warn("Wrong sndBuf value, {} instead of {}", () -> ch.config().getOption(ChannelOption.SO_SNDBUF) / 2, () -> sndBuf);
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
            Principal principal = checkSslClient(sslClientAuthentication, sess);
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
        engine.setUseClientMode(true);
        pipeline.addLast("ssl", new SslHandler(engine));
    }

    private SSLEngine getEngine() {
        SSLEngine engine;
        if (sslKeyAlias != null && ! sslKeyAlias.isEmpty()) {
            engine = sslContext.createSSLEngine(sslKeyAlias, DEFINEDSSLALIAS);
        } else {
            engine = sslContext.createSSLEngine();
        }
        engine.setSSLParameters(sslParams);

        if (sslClientAuthentication != null) {
            sslClientAuthentication.configureEngine(engine);
        }
        return engine;
    }

    private static Principal checkSslClient(ClientAuthentication sslClientAuthentication, SSLSession sess) throws GeneralSecurityException {
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

    @Override
    protected void configureServerBootStrap(ServerBootstrap bootstrap) {
        configureAbstractBootStrap(bootstrap);
        super.configureServerBootStrap(bootstrap);
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap) {
        configureAbstractBootStrap(bootstrap);
        super.configureBootStrap(bootstrap);
    }

}
