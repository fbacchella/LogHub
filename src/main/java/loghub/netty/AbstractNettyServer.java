package loghub.netty;

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;

public abstract class AbstractNettyServer<A extends ComponentFactory<B, C, D, E>, B extends AbstractBootstrap<B,C>,C extends Channel, D extends Channel, E extends Channel, F> extends Receiver implements HandlersSource<D, E> {

    private A factory;
    private int backlog = 128;
    private AbstractBootstrap<B,C> bootstrap;
    private ChannelFuture cf;

    public AbstractNettyServer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public String getReceiverName() {
        return "Netty";
    }

    @Override
    public boolean configure(Properties properties) {
        factory = getFactory(properties);
        SocketAddress address = getAddress();
        bootstrap = factory.getBootStrap();
        factory.group();
        if (factory.withChildHandler()) {
            factory.addChildhandlers(this);
            bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
        } else {
            factory.addHandlers(this);
        }
        // Bind and start to accept incoming connections.
        try {
            cf = bootstrap.bind(address).sync();
            logger.debug("{} started", () -> getAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return super.configure(properties);
    }

    protected abstract A getFactory(Properties properties);

    protected abstract ByteToMessageDecoder getNettyDecoder();

    protected abstract void populate(Event event, ChannelHandlerContext ctx, F msg);

    protected abstract SocketAddress getAddress();

    @Override
    public void run() {
        try {
            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            factory.finish();
        }
    }

    @Override
    public void addChildHandlers(E ch) {
        ch.pipeline().addLast(getNettyDecoder(), getSender());
    }

    @Override
    public void addHandlers(D ch) {
        // Default does nothing
    }

    protected ChannelInboundHandlerAdapter getSender() {
        return new SimpleChannelInboundHandler<F>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, F msg) throws Exception {
                Event event = emptyEvent();
                populate(event, ctx, msg);
                send(event);
            }
        };
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

}
