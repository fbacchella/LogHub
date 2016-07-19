package loghub.netty;

import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.ReflectionUtil;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import loghub.configuration.Properties;

public abstract class AbstractNettyServer<A extends ComponentFactory<B, C, D, E>, B extends AbstractBootstrap<B,C>,C extends Channel, D extends Channel, E extends Channel, F> {

    protected final Logger logger;
    private A factory;
    private int backlog = 128;
    private AbstractBootstrap<B,C> bootstrap;
    private SocketAddress address;

    public AbstractNettyServer() {
        logger = LogManager.getLogger(ReflectionUtil.getCallerClass(2));
    }

    public ChannelFuture configure(Properties properties, HandlersSource<D, E> source) {
        factory = getNewFactory(properties);
        address = getAddress();
        bootstrap = factory.getBootStrap();
        factory.group();
        if (factory.withChildHandler()) {
            factory.addChildhandlers(source);
            bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
        } else {
            factory.addHandlers(source);
        }
        // Bind and start to accept incoming connections.
        try {
            ChannelFuture cf = bootstrap.bind(address).sync();
            logger.debug("{} started", () -> getAddress());
            return cf;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    protected abstract A getNewFactory(Properties properties);

    protected abstract SocketAddress getAddress();

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public A getFactory() {
        return factory;
    }

}
