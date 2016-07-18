package loghub.netty;

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;

public abstract class NettyReceiver<S extends AbstractNettyServer<A, B, C, D, E, F>, A extends ComponentFactory<B, C, D, E>, B extends AbstractBootstrap<B,C>,C extends Channel, D extends Channel, E extends Channel, F> extends Receiver implements HandlersSource<D, E> {

    ChannelFuture cf;
    S server;

    public NettyReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        S server = getServer();
        cf = server.configure(properties);
        return cf != null && super.configure(properties);
    }


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
            server.getFactory().finish();
        }
    }

    @Override
    public void addChildHandlers(E ch) {
        ch.pipeline().addLast(getNettyDecoder(), getSender());
    }

    @Override
    public void addHandlers(D ch) {
        ch.pipeline().addLast(getNettyDecoder(), getSender());
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

    protected abstract ChannelInboundHandlerAdapter getNettyDecoder();

    protected abstract void populate(Event event, ChannelHandlerContext ctx, F msg);

    protected abstract SocketAddress getAddress();

    protected abstract S getServer();

}
