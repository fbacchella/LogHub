package loghub.netty;

import java.net.SocketAddress;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractNettyServer;
import loghub.receivers.Receiver;

public abstract class NettyReceiver<R extends NettyReceiver<R, S, B, CF, BS, BSC, SC, CC, SA, SM>,
                                    S extends AbstractNettyServer<CF, BS, BSC, SC, SA, S, B>,
                                    B extends AbstractNettyServer.Builder<S, B, BS, BSC>,
                                    CF extends ComponentFactory<BS, BSC, SA>,
                                    BS extends AbstractBootstrap<BS,BSC>, 
                                    BSC extends Channel,
                                    SC extends Channel,
                                    CC extends Channel,
                                    SA extends SocketAddress,
                                    SM> extends Receiver {

    protected static final AttributeKey<ConnectionContext<?  extends SocketAddress>> CONNECTIONCONTEXTATTRIBUTE = AttributeKey.newInstance(ConnectionContext.class.getName());

    protected S server;
    private int threadsCount = 1;
    private String poller = "NIO";

    public NettyReceiver() {
        super();
    }

    public final boolean configure(Properties properties) {
        return configure(properties, getServerBuilder());
    }

    protected abstract B getServerBuilder();

    public boolean configure(Properties properties, B builder) {
        builder.setAuthHandler(getAuthHandler(properties)).setWorkerThreads(threadsCount).setPoller(poller);
        builder.setConsumer(AbstractNettyServer.resolveConsumer(this));
        try {
            server = builder.build();
            return super.configure(properties);
        } catch (IllegalStateException ex) {
            logger.error("Can't start receiver: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return false;
        } catch (InterruptedException e1) {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            // Wait until the server socket is closed.
            server.waitClose();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    @Override
    public void stopReceiving() {
        server.close();
        super.stopReceiving();
    }

    public abstract ByteBuf getContent(SM message);

    public Event nettyMessageDecode(ChannelHandlerContext ctx, SM message) {
        ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
        return decode(cctx, getContent(message));
    }

    public boolean nettySend(Event e) {
        return send(e);
    }

    public final SA getListenAddress() {
        return server.getAddress();
    }

    @SuppressWarnings("unchecked")
    public ConnectionContext<SA> getConnectionContext(ChannelHandlerContext ctx, SM message) {
        ConnectionContext<SA> cctx;
        if (ctx.channel().hasAttr(CONNECTIONCONTEXTATTRIBUTE)) {
            cctx = (ConnectionContext<SA>) ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
        } else {
            cctx = getNewConnectionContext(ctx, message);
            ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        }
        Optional.ofNullable(ctx.channel().attr(AbstractNettyServer.PRINCIPALATTRIBUTE).get()).ifPresent(cctx::setPrincipal);
        return cctx;
    }

    public abstract ConnectionContext<SA> getNewConnectionContext(ChannelHandlerContext ctx, SM message);

    @Override
    public void close() {
        if (server != null) {
            server.close();
        }
        super.close();
    }

    /**
     * @return the threads
     */
    public int getWorkerThreads() {
        return threadsCount;
    }

    /**
     * @param threads the threads to set
     */
    public void setWorkerThreads(int threads) {
        this.threadsCount = threads;
    }

    /**
     * @return the poller
     */
    public String getPoller() {
        return poller;
    }

    /**
     * @param poller the poller to set
     */
    public void setPoller(String poller) {
        this.poller = poller;
    }

}
