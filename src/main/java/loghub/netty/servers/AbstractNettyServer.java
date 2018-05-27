package loghub.netty.servers;

import java.net.SocketAddress;
import java.security.Principal;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.ComponentFactory;
import loghub.netty.POLLER;
import loghub.security.AuthenticationHandler;

/**
 * @author Fabrice Bacchella
 *
 * @param <CF>  ComponentFactory
 * @param <BS>  BootStrap (Bootstrap, ServerBootstrap)
 * @param <BSC> Bootstrap's channel (Channel or ServerChannel)
 * @param <SC>  Server channel
 * @param <SA>  Socket Address
 */
public abstract class AbstractNettyServer<CF extends ComponentFactory<BS, BSC, SA>,
                                          BS extends AbstractBootstrap<BS, BSC>,
                                          BSC extends Channel,
                                          SC extends Channel,
                                          SA extends SocketAddress,
                                          S extends AbstractNettyServer<CF, BS, BSC, SC, SA, S, B>,
                                          B extends AbstractNettyServer.Builder<S, B>
                                         > {

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static final AttributeKey<Principal> PRINCIPALATTRIBUTE = AttributeKey.newInstance(Principal.class.getName());

    public static final int DEFINEDSSLALIAS=-2;

    public static abstract class Builder<S extends AbstractNettyServer<?, ?, ?, ?, ?, S, B>,
                                         B extends AbstractNettyServer.Builder<S, B>
                                        > {
        AuthenticationHandler authHandler = null;
        int threadsCount;
        POLLER poller = POLLER.NIO;
        @SuppressWarnings("unchecked")
        public B setAuthHandler(AuthenticationHandler authHandler) {
            this.authHandler = authHandler;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setWorkerThreads(int threadsCount) {
            this.threadsCount = threadsCount;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setPoller(String poller) {
            this.poller = POLLER.valueOf(poller);
            return (B) this;
        }
       public abstract S build();
    }

    protected final Logger logger;
    private CF factory;
    private final SA address;
    protected final POLLER poller;
    private int threadsCount;
    private ThreadFactory threadFactory = null;
    protected final AuthenticationHandler authHandler;

    public AbstractNettyServer(B builder) {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
        authHandler = builder.authHandler;
        threadsCount = builder.threadsCount;
        poller = builder.poller;
        address = setAddress(builder);
    }

    protected abstract SA setAddress(B builder);

    public boolean configure(ChannelConsumer<BS, BSC> consumer) {
        if (address == null) {
            logger.warn("Can't get listening address from {}", consumer);
            return false;
        }
        factory = getNewFactory();
        BS bootstrap = factory.getBootStrap();
        configureBootStrap(bootstrap);
        factory.group(threadsCount, threadFactory);
        factory.addChildhandlers(consumer, this);
        factory.addHandlers(consumer, this);
        consumer.addOptions((BS) bootstrap);
        logger.debug("started {} with consumer {} listening on {}", factory, consumer, address);
        return makeChannel(bootstrap, address);
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

    public void configureBootStrap(BS bootstrap) {

    }

    public void addHandlers(ChannelPipeline p) {
    }

    public CF getFactory() {
        return factory;
    };

    public int getWorkerThreads() {
        return threadsCount;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadsFactory) {
        this.threadFactory = threadsFactory;
    }

    public AuthenticationHandler getAuthHandler() {
        return authHandler;
    }

}
