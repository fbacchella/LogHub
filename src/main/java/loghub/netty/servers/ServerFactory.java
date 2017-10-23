package loghub.netty.servers;

import java.net.SocketAddress;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import loghub.netty.ChannelConsumer;
import loghub.netty.ComponentFactory;

public abstract class ServerFactory<CC extends Channel, SA extends SocketAddress> extends ComponentFactory<ServerBootstrap, ServerChannel, SA> {

    private static final Logger logger = LogManager.getLogger();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;

    @Override
    public AbstractBootstrap<ServerBootstrap, ServerChannel> getBootStrap() {
        bootstrap = new ServerBootstrap();
        bootstrap.channelFactory(getInstance());
        return bootstrap;
    }

    @Override
    public void group(int threads) {
        bossGroup = getEventLoopGroup(threads);
        workerGroup = getEventLoopGroup(threads);
        bootstrap.group(bossGroup, workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public void addChildhandlers(ChannelConsumer<ServerBootstrap, ServerChannel, SA> source) {
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

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                source.exception(ctx, cause);
            }
            
        };
        bootstrap.childHandler(handler);
    }

}
