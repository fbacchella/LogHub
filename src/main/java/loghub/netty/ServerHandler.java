package loghub.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import loghub.Helpers;
import loghub.Start;
import loghub.netty.servers.AbstractNettyServer;

public abstract class ServerHandler<CC extends Channel, SA extends SocketAddress> extends
        AbstractHandler<ServerBootstrap, ServerChannel, SA> {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;

    public ServerHandler(POLLER poller) {
        super(poller);
    }

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
    public Runnable finisher() {
        return () -> {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        };
    }

    @Override
    public void addChildhandlers(ChannelConsumer<ServerBootstrap, ServerChannel> source, AbstractNettyServer<?, ServerBootstrap, ServerChannel, ?, SA, ?, ?> server, Logger logger) {
        ChannelHandler handler = new ChannelInitializer<CC>() {
            @Override
            public void initChannel(CC ch) throws Exception {
                server.addHandlers(ch.pipeline());
                if (server != source) {
                    source.addHandlers(ch.pipeline());
                }
                ServerHandler.this.addErrorHandler(ch.pipeline(), logger);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                if (Helpers.isFatal(cause)) {
                    source.logFatalException(cause);
                    Start.fatalException(cause);
                } else {
                    source.exception(ctx, cause);
                }
            }

        };
        bootstrap.childHandler(handler);
    }

}
