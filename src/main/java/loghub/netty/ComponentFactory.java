package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;

public abstract class ComponentFactory<B extends AbstractBootstrap<B,C>,C extends Channel> {
    public abstract EventLoopGroup getEventLoopGroup();
    public abstract ChannelFactory<C> getInstance();
    public abstract AbstractBootstrap<B,C> getBootStrap();
    public abstract void group();
    public abstract void finish();
    public abstract boolean withChildHandler();
    public abstract void addChildhandlers(HandlersSource source);
}
