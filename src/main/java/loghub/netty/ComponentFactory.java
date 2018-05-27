package loghub.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import loghub.netty.servers.AbstractNettyServer;

public abstract class ComponentFactory<BS extends AbstractBootstrap<BS,BSC>, BSC extends Channel, SA extends SocketAddress> {
    public abstract EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory);
    public abstract ChannelFactory<BSC> getInstance();
    public abstract BS getBootStrap();
    public abstract void group(int threads, ThreadFactory threadFactory);
    public abstract void finish();
    public abstract void addChildhandlers(ChannelConsumer<BS, BSC> source, AbstractNettyServer<?, BS, BSC, ?, SA, ?, ?> server);
    public void addHandlers(ChannelConsumer<BS, BSC> source, AbstractNettyServer<?, BS, BSC, ?, SA, ?, ?> server) { };
}
