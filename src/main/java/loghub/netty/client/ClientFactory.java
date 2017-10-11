package loghub.netty.client;

import java.net.SocketAddress;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import loghub.netty.ChannelConsumer;
import loghub.netty.ComponentFactory;

public abstract class ClientFactory<CC extends Channel, SA extends SocketAddress> extends ComponentFactory<Bootstrap, Channel, SA> {

    private static final Logger logger = LogManager.getLogger();

    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;

    @Override
    public Bootstrap getBootStrap() {
        bootstrap = new Bootstrap();
        bootstrap.channelFactory(getInstance());
        return bootstrap;
    }

    @Override
    public void group() {
        workerGroup = getEventLoopGroup();
        bootstrap.group(workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
    }

    @Override
    public void addChildhandlers(ChannelConsumer<Bootstrap, Channel, SA> source) {
    }

    @Override
    public void addHandlers(ChannelConsumer<Bootstrap, Channel, SA> source) {
        ChannelHandler handler = new ChannelInitializer<CC>() {
            @Override
            public void initChannel(CC ch) throws Exception {
                try {
                    source.addHandlers(ch.pipeline());
                } catch (Exception e) {
                    logger.error("Netty handler failed: {}", e.getMessage());
                    logger.throwing(Level.DEBUG, e);
                }
            }
        };
        bootstrap.handler(handler);
    }

}
