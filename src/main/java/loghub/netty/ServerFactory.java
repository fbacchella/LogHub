package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;

public abstract class ServerFactory extends ComponentFactory<ServerBootstrap,ServerChannel> {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;

    @Override
    public AbstractBootstrap<ServerBootstrap, ServerChannel> getBootStrap() {
        bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.channelFactory(getInstance());
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    @Override
    public void group() {
        bossGroup = getEventLoopGroup();
        workerGroup = getEventLoopGroup();
        bootstrap.group(bossGroup, workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public boolean withChildHandler() {
        return true;
    }

    public void addChildhandlers(HandlersSource source) {
        ChannelHandler handler = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                source.addChildHandlers(ch);
            }
        };
        bootstrap.childHandler(handler);
    }

}
