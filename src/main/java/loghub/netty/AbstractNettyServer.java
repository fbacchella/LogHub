package loghub.netty;

import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import loghub.Helpers;
import loghub.configuration.Properties;

/**
 * @author fa4
 *
 * @param <CF>  ComponentFactory
 * @param <BS>  BootStrap (Bootstrap, ServerBootstrap)
 * @param <BSC> Bootstrap's channel (Channel or ServerChannel)
 * @param <SC>  Server channel
 * @param <CC>  Client channel
 * @param <SA>  Socket Address
 */
public abstract class AbstractNettyServer<CF extends ComponentFactory<BS, BSC, SA>, BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel, SC extends Channel, SA extends SocketAddress> {

    protected final Logger logger;
    private CF factory;
    private AbstractBootstrap<BS,BSC> bootstrap;
    private SA address;
    protected POLLER poller = POLLER.NIO;

    public AbstractNettyServer() {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    @SuppressWarnings("unchecked")
    public ChannelFuture configure(Properties properties, ChannelConsumer<BS, BSC, SA> consumer) {
        address = consumer.getListenAddress();
        factory = getNewFactory(properties);
        bootstrap = factory.getBootStrap();
        factory.group();
        factory.addChildhandlers(consumer);
        factory.addHandlers(consumer);
        consumer.addOptions((BS) bootstrap);
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

    protected abstract CF getNewFactory(Properties properties);

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

}
