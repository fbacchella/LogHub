package loghub.netty;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.Stats;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.netty.servers.AbstractNettyServer;
import loghub.receivers.Receiver;
import lombok.Setter;

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

    public abstract static class Builder<B extends NettyReceiver<?, ?, ?, ?,?, ?, ?, ?, ?, ?>> extends Receiver.Builder<B> {
        @Setter
        String poller = "NIO";
        @Setter
        int workerThreads = 1;
    };

    protected S server;
    private final int workerThreads;
    private final String poller;

    protected NettyReceiver(Builder<? extends NettyReceiver<R, S, B, CF, BS, BSC, SC, CC, SA, SM>> builder) {
        super(builder);
        this.workerThreads = builder.workerThreads;
        this.poller = builder.poller;
    }

    @Override
    public final boolean configure(Properties properties) {
        return configure(properties, getServerBuilder());
    }

    protected abstract B getServerBuilder();

    public boolean configure(Properties properties, B builder) {
        try {
            builder.setAuthHandler(getAuthHandler(properties)).setWorkerThreads(workerThreads).setPoller(poller);
        } catch (IllegalArgumentException ex) {
            logger.error("Can't start receiver authentication handler: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return false;
        }
        builder.setConsumer(AbstractNettyServer.resolveConsumer(this));
        try {
            server = builder.build();
            return super.configure(properties);
        } catch (IllegalStateException ex) {
            logger.error("Can't start receiver: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return false;
        } catch (InterruptedException e1) {
            interrupt();
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

    public Stream<Event> nettyMessageDecode(ChannelHandlerContext ctx, ByteBuf message) {
        ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
        return decodeStream(cctx, message);
    }

    public boolean nettySend(Event e) {
        return send(e);
    }

    public final SA getListenAddress() {
        return server.getAddress();
    }

    public ConnectionContext<SA> makeConnectionContext(ChannelHandlerContext ctx, SM message) {
        ConnectionContext<SA> cctx = getNewConnectionContext(ctx, message);
        ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        Optional.ofNullable(ctx.channel().attr(AbstractNettyServer.PRINCIPALATTRIBUTE).get()).ifPresent(cctx::setPrincipal);
        return cctx;
    }

    @SuppressWarnings("unchecked")
    public ConnectionContext<SA> getConnectionContext(ChannelHandlerContext ctx) {
        return (ConnectionContext<SA>) ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
    }

    public abstract ConnectionContext<SA> getNewConnectionContext(ChannelHandlerContext ctx, SM message);

    @Override
    public void close() {
        if (server != null) {
            server.close();
        }
        super.close();
    }

    protected final Stream<Event> decodeStream(ConnectionContext<?> ctx, ByteBuf bbuf) {
        try {
            Stats.newReceivedMessage(this, bbuf.readableBytes());
            return decoder.decode(ctx, bbuf).map((m) -> mapToEvent(ctx, () -> bbuf != null && bbuf.isReadable(), () -> m)).filter(Objects::nonNull);
        } catch (DecodeException ex) {
            manageDecodeException(ex);
            return Stream.empty();
        }
    }

}
