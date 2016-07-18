package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;

public abstract class ComponentFactory<B extends AbstractBootstrap<B,C>, C extends Channel, D extends Channel, E extends Channel> {
    public abstract EventLoopGroup getEventLoopGroup();
    public abstract ChannelFactory<D> getInstance();
    public abstract AbstractBootstrap<B,C> getBootStrap();
    public abstract void group();
    public abstract void finish();
    public abstract boolean withChildHandler();
    public abstract void addChildhandlers(HandlersSource<D, E> source);
    public abstract void addHandlers(HandlersSource<D, E> source);
}
