package loghub.netty.servers;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import loghub.Helpers;
import loghub.netty.ChannelConsumer;
import loghub.netty.ComponentFactory;

public abstract class ServerFactory<CC extends Channel, SA extends SocketAddress> extends ComponentFactory<ServerBootstrap, ServerChannel, SA> {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;

    @Override
    public ServerBootstrap getBootStrap() {
        bootstrap = new ServerBootstrap();
        bootstrap.channelFactory(getInstance());
        return bootstrap;
    }

    @Override
    public void group(int threads, ThreadFactory threadFactory) {
        bossGroup = getEventLoopGroup(threads, threadFactory);
        workerGroup = getEventLoopGroup(threads, threadFactory);
        bootstrap.group(bossGroup, workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public void addChildhandlers(ChannelConsumer<ServerBootstrap, ServerChannel> source, AbstractNettyServer<?, ServerBootstrap, ServerChannel, ?, SA, ?, ?> server) {
        ChannelHandler handler = new ChannelInitializer<CC>() {
            @Override
            public void initChannel(CC ch) throws Exception {
                server.addHandlers(ch.pipeline());
                source.addHandlers(ch.pipeline());
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                if (Helpers.isFatal(cause)) {
                    throw (Exception) cause;
                } else {
                    source.exception(ctx, cause);
                }
            }

        };
        bootstrap.childHandler(handler);
    }

}
