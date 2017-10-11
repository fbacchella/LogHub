package loghub.netty.client;

import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;

/**
 * @author Fabrice Bacchella
 *
 * @param <CF>  ComponentFactory
 * @param <BS>  BootStrap (Bootstrap, ServerBootstrap)
 * @param <BSC> Bootstrap's channel (Channel or ServerChannel)
 * @param <SC>  Server channel
 * @param <SA>  Socket Address
 */
public abstract class AbstractNettyClient<CF extends ClientFactory<CC, SA>, CC extends Channel, SA extends SocketAddress> {

    protected final Logger logger;
    private CF factory;
    private Bootstrap bootstrap;
    private SA address;

    public AbstractNettyClient() {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    public ChannelFuture configure(Properties properties, ChannelConsumer<Bootstrap, Channel, SA> consumer) {
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
            ChannelFuture cf = bootstrap.connect(address).sync();
            logger.debug("started {} with consumer {} listening on {}", factory, consumer, address);
            return cf;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    protected abstract CF getNewFactory(Properties properties);

    public SA getAddress() {
        return address;
    }

    public void finish() {
    }

    public void configureBootStrap(Bootstrap bootstrap) {

    }

    public ClientFactory<CC, SA> getFactory() {
        return factory;
    };

}
