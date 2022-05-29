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
import loghub.ThreadBuilder;
import loghub.netty.AbstractHandler;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
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
public abstract class AbstractNettyServer<CF extends AbstractHandler<BS, BSC, SA>,
                                          BS extends AbstractBootstrap<BS, BSC>,
                                          BSC extends Channel,
                                          SC extends Channel,
                                          SA extends SocketAddress,
                                          S extends AbstractNettyServer<CF, BS, BSC, SC, SA, S, B>,
                                          B extends AbstractNettyServer.Builder<S, B, BS, BSC>
                                         > {

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static final AttributeKey<Principal> PRINCIPALATTRIBUTE = AttributeKey.newInstance(Principal.class.getName());

    public static final int DEFINEDSSLALIAS=-2;

    public abstract static class Builder<S extends AbstractNettyServer<?, ?, ?, ?, ?, S, B>,
                                         B extends AbstractNettyServer.Builder<S, B, BS, BSC>,
                                         BS extends AbstractBootstrap<BS, BSC>,
                                         BSC extends Channel
                                        > {
        AuthenticationHandler authHandler = null;
        int threadsCount;
        POLLER poller = POLLER.DEFAULTPOLLER;
        ChannelConsumer<BS, BSC> consumer;
        String threadPrefix;
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
            this.poller = POLLER.resolve(poller);
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setConsumer(ChannelConsumer<BS, BSC> consumer) {
            this.consumer = consumer;
            return (B) this;
        }
        @SuppressWarnings("unchecked")
        public B setThreadPrefix(String threadPrefix) {
            this.threadPrefix = threadPrefix;
            return (B) this;
        }
        public abstract S build() throws IllegalStateException, InterruptedException;
    }

    @SuppressWarnings("unchecked")
    public static <BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel> ChannelConsumer<BS, BSC> resolveConsumer(Object o) {
        if (o instanceof ChannelConsumer) {
            return (ChannelConsumer<BS, BSC>) o;
        } else if (o instanceof ConsumerProvider) {
            ConsumerProvider<?, BS, BSC> cp = (ConsumerProvider<?, BS, BSC>) o;
            return cp.getConsumer();
        } else {
            return null;
        }
    }

    protected final Logger logger;
    private final Runnable finisher;
    private final SA address;
    protected final POLLER poller;
    protected final AuthenticationHandler authHandler;

    public AbstractNettyServer(B builder) throws IllegalStateException, InterruptedException {
        if (builder.consumer == null) {
            builder.consumer = resolveConsumer(this);
        }
        if (builder.consumer == null) {
            throw new RuntimeException("Can't find channel consumer");
        }
        if (builder.threadPrefix == null) {
            throw new RuntimeException("Thread prefix not defined");
        }
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        authHandler = builder.authHandler;
        poller = builder.poller;
        address = resolveAddress(builder);
        if (address == null) {
            throw new RuntimeException("Can't get listening address");
        }
        CF factory = getNewFactory();
        finisher = factory.finisher();
        BS bootstrap = factory.getBootStrap();
        configureBootStrap(bootstrap, builder);
        ThreadFactory threadFactory = ThreadBuilder.get()
                                                   .setDaemon(true)
                                                   .getFactory(builder.threadPrefix + "/" + address.toString());
        factory.group(builder.threadsCount, threadFactory);
        factory.addChildhandlers(builder.consumer, this, logger);
        factory.addHandlers(builder.consumer, this, logger);
        builder.consumer.addOptions((BS) bootstrap);
        logger.debug("started {} with consumer {} listening on {}", factory, builder.consumer, address);
        makeChannel(bootstrap, address, builder);
    }

    protected abstract SA resolveAddress(B builder);

    protected abstract void makeChannel(AbstractBootstrap<BS,BSC> bootstrap, SA address, B builder) throws IllegalStateException, InterruptedException;

    public abstract void waitClose() throws InterruptedException;

    protected abstract CF getNewFactory();

    public SA getAddress() {
        return address;
    }

    public void close() {
        finisher.run();
    }

    public String getPoller() {
        return poller.toString();
    }

    public void configureBootStrap(BS bootstrap, B builder) {
    }

    public void addHandlers(ChannelPipeline p) {
    }

    public AuthenticationHandler getAuthHandler() {
        return authHandler;
    }

}
