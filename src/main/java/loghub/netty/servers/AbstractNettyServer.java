package loghub.netty.servers;

import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ServerChannel;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;
import loghub.netty.POLLER;

/**
 * @author fa4
 *
 * @param <CF>  ComponentFactory
 * @param <BS>  BootStrap (Bootstrap, ServerBootstrap)
 * @param <BSC> Bootstrap's channel (Channel or ServerChannel)
 * @param <SC>  Server channel
 * @param <SA>  Socket Address
 */
public abstract class AbstractNettyServer<SF extends ServerFactory<SC, SA>, SC extends ServerChannel, CC extends Channel, SA extends SocketAddress> {

    protected final Logger logger;
    private ServerFactory<SC, SA> factory;
    private ServerBootstrap bootstrap;
    private SA address;
    protected POLLER poller = POLLER.NIO;

    public AbstractNettyServer() {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    public ChannelFuture configure(Properties properties, ChannelConsumer<ServerBootstrap, ServerChannel, SA> consumer) {
        address = consumer.getListenAddress();
        if (address == null) {
            return null;
        }
        factory = getNewFactory(properties);
        bootstrap = factory.getBootStrap();
        configureBootStrap(bootstrap);
        factory.group();
        factory.addChildhandlers(consumer);
        factory.addHandlers(consumer);
        consumer.addOptions(bootstrap);
        // Bind and start to accept incoming connections.
        try {
            ChannelFuture cf = bootstrap.bind(address).sync();
            logger.debug("started {} with consumer {} listening on {}", factory, consumer, address);
            return cf;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    protected abstract ServerFactory<SC, SA> getNewFactory(Properties properties);

    public SA getAddress() {
        return address;
    }

    public void finish() {
        factory.finish();
    }

    public String getPoller() {
        return poller.toString();
    }

    public void setPoller(String poller) {
        this.poller = POLLER.valueOf(poller);
    }

    public void configureBootStrap(ServerBootstrap bootstrap) {

    }

    public ServerFactory<SC, SA> getFactory() {
        return factory;
    };

}
