package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;

public abstract class NettyServer<A extends ComponentFactory<B,C>, B extends AbstractBootstrap<B,C>,C extends Channel, D> extends Receiver implements HandlersSource {

    int port;
    private A factory;
    private int backlog = 128;

    public NettyServer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public String getReceiverName() {
        return "Netty";
    }

    @Override
    public boolean configure(Properties properties) {
        factory = getFactory(properties);
        return super.configure(properties);
    }

    protected abstract A getFactory(Properties properties);

    protected abstract ByteToMessageDecoder getNettyDecoder();

    protected abstract void populate(Event event, ChannelHandlerContext ctx, D msg);

    @Override
    public void run() {
        try {
            AbstractBootstrap<?, ?> b = factory.getBootStrap();
            factory.group();
            if (factory.withChildHandler()) {
                factory.addChildhandlers(this);
            }
            b.option(ChannelOption.SO_BACKLOG, backlog);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            factory.finish();
        }
    }

    @Override
    public void addChildHandlers(SocketChannel ch) {
        ch.pipeline().addLast(getNettyDecoder(), getSender());
    }

    @Override
    public void addHandlers(SocketChannel ch) {
        // Default does nothing
    }

    protected ChannelInboundHandlerAdapter getSender() {
        return new SimpleChannelInboundHandler<D>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, D msg) throws Exception {
                Event event = emptyEvent();
                populate(event, ctx, msg);
                send(event);
            }
        };
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

}
