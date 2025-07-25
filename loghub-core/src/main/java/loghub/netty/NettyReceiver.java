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
import loghub.Helpers;
import loghub.configuration.ConfigurationProperties;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.metrics.Stats;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TcpTransport;
import loghub.receivers.Receiver;
import lombok.Getter;
import lombok.Setter;

public abstract class NettyReceiver<R extends NettyReceiver<R, M, B>, M, B extends NettyReceiver.Builder<R, M, B>> extends Receiver<R, B> {

    public static final String POLLER_PROPERTY_NAME = "poller";

    protected static final AttributeKey<ConnectionContext<? extends SocketAddress>> CONNECTIONCONTEXTATTRIBUTE = AttributeKey.newInstance(ConnectionContext.class.getName());

    @Setter
    public abstract static class Builder<R extends NettyReceiver<R, M, B>, M, B extends NettyReceiver.Builder<R, M, B>> extends Receiver.Builder<R, B> {
        protected POLLER poller = null;
        protected int workerThreads = 1;
        protected TRANSPORT transport;
        protected int port;
        protected String host = null;
        protected int rcvBuf = -1;
        protected int sndBuf = -1;
        protected int backlog = -1;
        protected boolean noDelay = false;
        protected ConfigurationProperties properties;
    }

    protected final NettyTransport<?, M, ?, ?> transport;
    @Getter
    private final String listen;
    @Getter
    private final int port;
    protected NettyReceiver(B builder) {
        super(builder);
        NettyTransport.Builder<?, M, ?, ?> nettyBuilder = builder.transport.getBuilder();
        nettyBuilder.setWorkerThreads(builder.workerThreads);
        nettyBuilder.setThreadPrefix(getThreadPrefix(builder));
        nettyBuilder.setEndpoint(builder.host);
        nettyBuilder.setBacklog(builder.backlog);
        nettyBuilder.setConsumer(NettyTransport.resolveConsumer(this));
        if (nettyBuilder instanceof AbstractIpTransport.Builder) {
            @SuppressWarnings({"unchecked"})
            AbstractIpTransport.Builder<M, ?, ?> nettyIpBuilder = (AbstractIpTransport.Builder<M, ?, ?>) nettyBuilder;
            nettyIpBuilder.setPort(builder.port);
            nettyIpBuilder.setRcvBuf(builder.rcvBuf);
            nettyIpBuilder.setSndBuf(builder.sndBuf);
            if (builder.transport == TRANSPORT.TCP) {
                ((TcpTransport.Builder) nettyIpBuilder).setNoDelay(builder.noDelay);
            }
            if (isWithSSL()) {
                nettyIpBuilder.setWithSsl(true);
                nettyIpBuilder.setSslContext(getSslContext());
                nettyIpBuilder.setSslParams(getSslParams());
                nettyIpBuilder.setSslClientAuthentication(getSSLClientAuthentication());
                nettyIpBuilder.setSslKeyAlias(getSSLKeyAlias());
                nettyIpBuilder.setWithSsl(true);
            }
        }
        if (builder.poller != null) {
            nettyBuilder.setPoller(builder.poller);
        } else if (builder.properties != null && builder.properties.containsKey(POLLER_PROPERTY_NAME)) {
            String pollerName = builder.properties.get(POLLER_PROPERTY_NAME).toString();
            nettyBuilder.setPoller(POLLER.resolve(pollerName));
        } else {
            nettyBuilder.setPoller(POLLER.DEFAULTPOLLER);
        }
        tweakNettyBuilder(builder, nettyBuilder);
        this.transport = nettyBuilder.build();
        this.listen = builder.host;
        this.port = builder.port;
    }

    protected abstract String getThreadPrefix(B builder);

    protected void tweakNettyBuilder(B builder, NettyTransport.Builder<?, M, ?, ?> nettyTransportBuilder) {

    }

    @Override
    public boolean configure(Properties properties) {
        if (! transport.getPoller().isAvailable()) {
            logger.error("Unavailable poller {}", transport.getPoller());
            return false;
        }
        try {
            transport.bind();
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

    public ByteBuf getContent(M message) {
        throw new UnsupportedOperationException();
    }

    public Stream<Event> nettyMessageDecode(ChannelHandlerContext ctx, ByteBuf message) {
        ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
        return decodeStream(cctx, message);
    }

    public boolean nettySend(Event e) {
        return send(e);
    }

    public <A extends SocketAddress> ConnectionContext<A> makeConnectionContext(ChannelHandlerContext ctx, M message) {
        @SuppressWarnings("unchecked")
        ConnectionContext<A> cctx = (ConnectionContext<A>) transport.getNewConnectionContext(ctx, message);
        ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        Optional.ofNullable(ctx.channel().attr(NettyTransport.PRINCIPALATTRIBUTE)).map(Attribute::get).ifPresent(cctx::setPrincipal);
        return cctx;
    }

    @SuppressWarnings("unchecked")
    public <A extends SocketAddress> ConnectionContext<A> getConnectionContext(ChannelHandlerContext ctx) {
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
