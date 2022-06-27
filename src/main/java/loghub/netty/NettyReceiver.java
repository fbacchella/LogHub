package loghub.netty;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.metrics.Stats;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TransportConfig;
import loghub.receivers.Receiver;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

public abstract class NettyReceiver<M> extends Receiver {

    protected static final AttributeKey<ConnectionContext<?  extends SocketAddress>> CONNECTIONCONTEXTATTRIBUTE = AttributeKey.newInstance(ConnectionContext.class.getName());

    public abstract static class Builder<S extends NettyReceiver
                                        > extends Receiver.Builder<S> {
        @Setter
        POLLER poller = POLLER.DEFAULTPOLLER;
        @Setter
        int workerThreads = 1;
        @Setter
        TRANSPORT transport;
        @Setter
        private int port;
        @Setter
        private String host = null;
        @Setter
        int rcvBuf = -1;
        @Setter
        int sndBuf = -1;
        @Setter
        private int backlog = -1;
    }

    protected TransportConfig config;
    private final NettyTransport<?, M> transport;
    @Getter
    private final String listen;
    @Getter
    private final int port;
    protected NettyReceiver(Builder<? extends NettyReceiver> builder) {
        super(builder);
        this.transport = builder.transport.getInstance(builder.poller);
        this.listen = builder.host;
        this.port = builder.port;
        config = new TransportConfig();
        config.setWorkerThreads(builder.workerThreads)
              .setPort(builder.port)
              .setEndpoint(builder.host)
              .setRcvBuf(builder.rcvBuf)
              .setSndBuf(builder.sndBuf)
              .setBacklog(builder.backlog);
        if (isWithSSL()) {
            config.setWithSsl(true);
            config.setSslClientAuthentication(getSSLClientAuthentication())
                  .setSslKeyAlias(getSSLKeyAlias())
                  .setWithSsl(true);
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if (config.isWithSsl()) {
            config.setSslContext(properties.ssl);
        }
        try {
            config.setAuthHandler(getAuthHandler(properties));
        } catch (IllegalArgumentException ex) {
            logger.error("Can't start receiver authentication handler: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return false;
        }
        config.setConsumer(NettyTransport.resolveConsumer(this));
        try {
            transport.bind(config);
            // config is not needed any more
            config = null;
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
            transport.waitClose();
        } finally {
            close();
        }
    }

    @Override
    public void stopReceiving() {
        transport.close();
        super.stopReceiving();
    }

    public abstract ByteBuf getContent(M message);

    public Stream<Event> nettyMessageDecode(ChannelHandlerContext ctx, ByteBuf message) {
        ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
        return decodeStream(cctx, message);
    }

    public boolean nettySend(Event e) {
        return send(e);
    }

    public <A extends SocketAddress> ConnectionContext<A> makeConnectionContext(ChannelHandlerContext ctx, M message) {
        ConnectionContext<A> cctx = (ConnectionContext<A>) transport.getNewConnectionContext(ctx, message);
        ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        Optional.ofNullable(ctx.channel().attr(NettyTransport.PRINCIPALATTRIBUTE)).map(Attribute::get).ifPresent(cctx::setPrincipal);
        return cctx;
    }

    @SuppressWarnings("unchecked")
    public<A extends SocketAddress> ConnectionContext<A> getConnectionContext(ChannelHandlerContext ctx) {
        return (ConnectionContext<A>) Optional.ofNullable(ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE)).map(Attribute::get).orElse(null);
    }


    @Override
    public void close() {
        if (transport != null) {
            transport.close();
        }
        super.close();
    }

    protected final Stream<Event> decodeStream(ConnectionContext<?> ctx, ByteBuf bbuf) {
        try {
            Stats.newReceivedMessage(this, bbuf.readableBytes());
            return decoder.decode(ctx, bbuf).map(m -> mapToEvent(ctx, m)).filter(Objects::nonNull);
        } catch (DecodeException ex) {
            manageDecodeException(ex);
            return Stream.empty();
        }
    }

}
