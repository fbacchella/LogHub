package loghub.netty.servers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.FailedLoginException;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.netty.ComponentFactory;
import loghub.security.ssl.ClientAuthentication;

public abstract class NettyIpServer<CF extends ComponentFactory<BS, BSC, InetSocketAddress>,
                                    BS extends AbstractBootstrap<BS, BSC>,
                                    BSC extends Channel,
                                    SC extends Channel,
                                    S extends NettyIpServer<CF, BS, BSC, SC, S, B>,
                                    B extends NettyIpServer.Builder<S, B, BS, BSC>
                                   > extends AbstractNettyServer<CF, BS, BSC, SC, InetSocketAddress, S, B> {

    public static final AttributeKey<SSLSession> SSLSESSIONATTRIBUTE = AttributeKey.newInstance(SSLSession.class.getName());

    public abstract static class Builder<S extends NettyIpServer<?, ?, ?, ?, S, B>,
                                         B extends Builder<S, B, BS, BSC>,
                                         BS extends AbstractBootstrap<BS, BSC>,
                                         BSC extends Channel
                                        > extends AbstractNettyServer.Builder <S, B, BS, BSC>{
        int port = -1;
        String host = null;
        SSLContext sslctx = null;
        boolean useSSL = false;
        ClientAuthentication sslClientAuthentication = ClientAuthentication.NONE;
        String sslKeyAlias = null;
        int rcvBuf = -1;
        int sndBuf = -1;
        protected Builder() {
        }
        @SuppressWarnings("unchecked")
        public B setHost(String host) {
            this.host = host != null && !host.isEmpty() ? host : null;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setPort(int port) {
            this.port = port;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setSSLContext(SSLContext sslctx) {
            this.sslctx = sslctx;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B useSSL(boolean useSSL) {
            this.useSSL = useSSL;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setSSLClientAuthentication(ClientAuthentication sslClientAuthentication) {
            this.sslClientAuthentication = sslClientAuthentication;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setSSLKeyAlias(String alias) {
            this.sslKeyAlias = alias;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setRcvBuf(int rcvBuf) {
            this.rcvBuf = rcvBuf;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setSndBuf(int sndBuf) {
            this.sndBuf = sndBuf;
            return (B) this;
        }
    }

    private SSLContext sslctx = null;
    private String sslKeyAlias = null;
    private ClientAuthentication sslClientAuthentication = null;
    private final int port;
    private final String host;

    protected NettyIpServer(B builder) throws IllegalArgumentException, InterruptedException {
        super(builder);
        port = builder.port;
        host = builder.host;
        if (builder.useSSL) {
            sslctx = builder.sslctx;
            sslClientAuthentication = builder.sslClientAuthentication;
            sslKeyAlias = builder.sslKeyAlias;
        }
    }

    @Override
    protected InetSocketAddress resolveAddress(B builder) {
        try {
            return new InetSocketAddress(builder.host != null ? InetAddress.getByName(builder.host) : null , builder.port);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", builder.host);
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

    @Override
    public void addHandlers(ChannelPipeline p) {
        super.addHandlers(p);
        if (isWithSSL()) {
            addSslHandler(p);
        }
    }

    @Override
    public void configureBootStrap(BS bootstrap, B builder) {
        if (builder.rcvBuf >= 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, builder.rcvBuf);
        }
        if (builder.sndBuf >= 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, builder.sndBuf);
        }
        super.configureBootStrap(bootstrap, builder);
    }

    public void addSslHandler(ChannelPipeline p) {
        logger.debug("adding an SSL handler on {}", () -> p.channel());
        SSLEngine engine = getEngine();
        SslHandler sslHandler = new SslHandler(engine);
        p.addFirst("ssl", sslHandler);
        Future<Channel> future = sslHandler.handshakeFuture();
        future.addListener(new GenericFutureListener<Future<Channel>>() {
            @Override
            public void operationComplete(Future<Channel> future) throws Exception {
                SSLSession sess = sslHandler.engine().getSession();
                logger.trace("SSL started with {}", () -> sess);
                Principal p = checkSslClient(sess);
                logger.debug("Got SSL client identity {}", () -> (p != null ? p.getName() : ""));
                if (p != null) {
                    future.get().attr(PRINCIPALATTRIBUTE).set(p);
                }
                future.get().attr(SSLSESSIONATTRIBUTE).set(sess);
            }});
    }

    public Principal checkSslClient(SSLSession sess) throws GeneralSecurityException {
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

    public SSLEngine getEngine() {
        SSLEngine engine;
        if (sslKeyAlias != null && ! sslKeyAlias.isEmpty()) {
            engine = sslctx.createSSLEngine(sslKeyAlias, DEFINEDSSLALIAS);
        } else {
            engine = sslctx.createSSLEngine();
        }

        SSLParameters params = engine.getSSLParameters();
        String[] protocols = Arrays.stream(params.getProtocols()).filter(s -> ! s.startsWith("SSL")).filter(s -> ! "TLSv1".equals(s)).filter(s -> ! "TLSv1.1".equals(s)).toArray(String[]::new);
        params.setProtocols(protocols);
        String[] cipherSuites = Arrays.stream(params.getCipherSuites()).filter(s -> ! s.contains("CBC")).toArray(String[]::new);
        params.setCipherSuites(cipherSuites);
        params.setUseCipherSuitesOrder(true);
        engine.setSSLParameters(params);

        engine.setUseClientMode(false);
        sslClientAuthentication.configureEngine(engine);
        return engine;
    }

    public boolean isWithSSL() {
        return sslctx != null;
    }

    /**
     * @return the sslClientAuthentication
     */
    public ClientAuthentication getSslClientAuthentication() {
        return sslClientAuthentication;
    }

    public String getSslKeyAlias() {
        return sslKeyAlias;
    }

}
