package loghub.netty.servers;

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.ComponentFactory;
import loghub.netty.POLLER;
import loghub.security.ssl.ClientAuthentication;

/**
 * @author fa4
 *
 * @param <CF>  ComponentFactory
 * @param <BS>  BootStrap (Bootstrap, ServerBootstrap)
 * @param <BSC> Bootstrap's channel (Channel or ServerChannel)
 * @param <SC>  Server channel
 * @param <SA>  Socket Address
 */
public abstract class AbstractNettyServer<CF extends ComponentFactory<BS, BSC, SA>, BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel, SC extends Channel, SA extends SocketAddress> {

    public static final int DEFINEDSSLALIAS=-2;

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static final AttributeKey<SSLSession> SSLSESSIONATTRIBUTE = AttributeKey.newInstance(SSLSession.class.getName());

    protected final Logger logger;
    private CF factory;
    private AbstractBootstrap<BS,BSC> bootstrap;
    private SA address;
    protected POLLER poller = POLLER.NIO;
    private int threadsCount;
    private ThreadFactory threadFactory;
    private SSLContext sslctx = null;
    private String sslKeyAlias = null;
    private ClientAuthentication sslClientAuthentication = null;

    public AbstractNettyServer() {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    @SuppressWarnings("unchecked")
    public boolean configure(ChannelConsumer<BS, BSC, SA> consumer) {
        address = consumer.getListenAddress();
        if (address == null) {
            return false;
        }
        factory = getNewFactory();
        bootstrap = factory.getBootStrap();
        configureBootStrap(bootstrap);
        factory.group(threadsCount, threadFactory);
        factory.addChildhandlers(consumer);
        factory.addHandlers(consumer);
        consumer.addOptions((BS) bootstrap);
        logger.debug("started {} with consumer {} listening on {}", factory, consumer, address);
        return makeChannel(bootstrap, address);
    }

    public void addSslHandler(ChannelPipeline p, SSLEngine engine) {
        logger.debug("adding an ssl handler on {}", p.channel());
        SslHandler sslHandler = new SslHandler(engine);
        p.addFirst("ssl", sslHandler);
        Future<Channel> future = sslHandler.handshakeFuture();
        future.addListener(new GenericFutureListener<Future<Channel>>() {
            @Override
            public void operationComplete(Future<Channel> future) throws Exception {
                try {
                    future.get().attr(SSLSESSIONATTRIBUTE).set(sslHandler.engine().getSession());
                } catch (ExecutionException e) {
                    logger.warn("Failed ssl connexion", e.getCause());
                    logger.catching(Level.DEBUG, e.getCause());
                }
            }});
    }

    protected abstract boolean makeChannel(AbstractBootstrap<BS,BSC> bootstrap, SA address);

    public abstract void waitClose() throws InterruptedException;

    protected abstract CF getNewFactory();

    public SA getAddress() {
        return address;
    }

    public void close() {
        factory.finish();
    }

    public String getPoller() {
        return poller.toString();
    }

    public void setPoller(String poller) {
        this.poller = POLLER.valueOf(poller);
    }

    public void configureBootStrap(AbstractBootstrap<BS,BSC> bootstrap) {

    }

    public CF getFactory() {
        return factory;
    };

    public int getWorkerThreads() {
        return threadsCount;
    }

    public void setWorkerThreads(int threads) {
        this.threadsCount = threads;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadsFactory) {
        this.threadFactory = threadsFactory;
    }

    public SSLEngine getEngine() {
        SSLEngine engine;
        if (sslKeyAlias != null && ! sslKeyAlias.isEmpty()) {
            engine = sslctx.createSSLEngine(sslKeyAlias, DEFINEDSSLALIAS);
        } else {
            engine = sslctx.createSSLEngine();
        }
        engine.setUseClientMode(false);
        sslClientAuthentication.configureEngine(engine);
        return engine;
    }

    public void setSSLContext(SSLContext sslctx) {
        this.sslctx = sslctx;
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

    /**
     * @param sslClientAuthentication the sslClientAuthentication to set
     */
    public void setSSLClientAuthentication(ClientAuthentication sslClientAuthentication) {
        this.sslClientAuthentication = sslClientAuthentication;
    }

    public String getSslKeyAlias() {
        return sslKeyAlias;
    }

    public void setSSLKeyAlias(String sslKeyAlias) {
        this.sslKeyAlias = sslKeyAlias;
    }

}
