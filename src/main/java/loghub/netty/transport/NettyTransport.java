package loghub.netty.transport;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.Principal;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.unix.UnixChannelOption;
import io.netty.handler.codec.CodecException;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.util.AttributeKey;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.Start;
import loghub.ThreadBuilder;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import lombok.Getter;
import lombok.Setter;

public abstract class NettyTransport<SA extends SocketAddress, M, T extends NettyTransport<SA, M, T, B>, B extends NettyTransport.Builder<SA, M, T, B>> {

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static final AttributeKey<Principal> PRINCIPALATTRIBUTE = AttributeKey.newInstance(Principal.class.getName());
    public static final String ERROR_HANDLER_NAME = "errorhandler";

    public abstract static class Builder<SA extends SocketAddress, M, T extends NettyTransport<SA, M, T, B>, B extends NettyTransport.Builder<SA, M, T, B>> {
        @Setter
        protected int backlog;
        @Setter
        protected int bufferSize = -1;
        @Setter
        protected int workerThreads;
        @Setter
        protected ChannelConsumer consumer;
        @Setter
        protected String threadPrefix;
        @Setter
        protected String endpoint;
        @Setter
        protected int timeout = 1;
        @Setter
        protected POLLER poller = POLLER.DEFAULTPOLLER;
        protected Builder() {
            // Not public constructor
        }
        public abstract T build();
    }

    @ChannelHandler.Sharable
    private static class ErrorHandler extends SimpleChannelInboundHandler<Object> {
        private final Logger logger;
        private ErrorHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                Object msg) {
            logger.warn("Not processed message {}", msg);
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause.getCause() instanceof NotSslRecordException) {
                logger.warn("Not a SSL connexion from {} on SSL listen", () -> ctx.channel().remoteAddress());
            } else if (cause.getCause() instanceof SSLHandshakeException) {
                logger.warn("Failed SSL handshake from {}: {}", () -> ctx.channel().remoteAddress(), () -> Helpers.resolveThrowableException(cause));
                logger.catching(Level.DEBUG, cause);
            } else if (cause instanceof IOException) {
                logger.warn("IO exception from {}: {}", () -> ctx.channel().remoteAddress(), () -> Helpers.resolveThrowableException(cause));
                logger.catching(Level.DEBUG, cause);
            } else if (cause instanceof CodecException) {
                logger.warn("Codec exception from {}: {}", () -> ctx.channel().remoteAddress(), () -> Helpers.resolveThrowableException(cause));
                logger.catching(Level.DEBUG, cause);
            } else {
                logger.warn("Not handled exception {} from {}", () -> Helpers.resolveThrowableException(cause), () -> ctx.channel().remoteAddress());
                logger.throwing(Level.WARN, cause);
            }
            if (! (ctx.channel() instanceof DatagramChannel)) {
                // UDP should not be close
                ctx.close();
                logger.warn("channel closed");
            }
        }

    }

    @Getter
    protected final String endpoint;
    @Getter
    protected final int backlog;
    @Getter
    protected final int bufferSize;
    @Getter
    protected final int workerThreads;
    @Getter
    protected final ChannelConsumer consumer;
    @Getter
    protected final String threadPrefix;
    @Getter
    protected final int timeout;
    @Getter
    protected final TRANSPORT transport;
    @Getter
    protected final POLLER poller;

    protected final Logger logger = LogManager.getLogger(Helpers.getFirstInitClass());

    private Optional<Runnable> finisher = Optional.empty();
    private Future<Boolean> finished = CompletableFuture.completedFuture(true);
    private final Set<ChannelFuture> listeningChannels = new HashSet<>(1);

    protected NettyTransport(B b) {
        this.transport = getClass().getAnnotation(TransportEnum.class).value();
        this.poller = b.poller;
        this.endpoint = b.endpoint;
        this.backlog = b.backlog;
        this.bufferSize = b.bufferSize;
        this.workerThreads = b.workerThreads;
        this.consumer = b.consumer;
        this.threadPrefix = b.threadPrefix;
        this.timeout = b.timeout;
    }

    public ChannelFuture connect() throws InterruptedException {
        SocketAddress address = resolveAddress();
        if (address == null) {
            throw new IllegalArgumentException("Can't get listening address: " + "connect");
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channelFactory(() -> poller.clientChannelProvider(transport));
        ThreadFactory threadFactory = ThreadBuilder.get()
                                              .setDaemon(true)
                                              .getFactory(getStringPrefix() + "/" + address);
        EventLoopGroup workerGroup = poller.getEventLoopGroup(1, threadFactory);
        bootstrap.group(workerGroup);
        addHandlers(bootstrap);
        finisher = Optional.of(workerGroup::shutdownGracefully);
        finisher.ifPresent(r -> finished = new FutureTask<>(r, true));
        consumer.addOptions(bootstrap);
        configureBootStrap(bootstrap);
        ChannelFuture cf = bootstrap.connect(address);
        cf.await();
        listeningChannels.add(cf);
        logger.debug("Connected to {}", address);
        return cf;
    }

    public void bind() throws InterruptedException {
        SocketAddress address = resolveAddress();
        if (address == null) {
            throw new IllegalArgumentException("Can't get listening address: " + "listen");
        }
        if (transport.isConnectedServer()) {
            bindConnected(address);
        } else {
            bindConnectionless(address);
        }
        for (ChannelFuture cf: listeningChannels) {
            try {
                cf.await().channel();
                cf.get();
            } catch (ExecutionException | InterruptedException e) {
                listeningChannels.stream().map(ChannelFuture::channel).forEach(Channel::close);
                listeningChannels.clear();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw (InterruptedException) e;
                } else {
                    throw new IllegalStateException("Failed to start listening on " + address, e.getCause());
                }
            }
        }
        finisher.ifPresent(r -> finished = new FutureTask<>(r, true));
        logger.debug("Bond to {}", address);
    }

    private void bindConnectionless(SocketAddress address) {
        Bootstrap bootstrap = new Bootstrap();
        int localWorkersThread = this.workerThreads;
        bootstrap.channelFactory(() -> poller.clientChannelProvider(transport));
        if (bufferSize > 0) {
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(bufferSize));
        }
        // Needed because Netty's UDP is not multi-thread, see http://marrachem.blogspot.fr/2014/09/multi-threaded-udp-server-with-netty-on.html
        if (poller.isUnixSocket() && workerThreads > 1) {
            bootstrap.option(UnixChannelOption.SO_REUSEPORT, true);
        } else if (!poller.isUnixSocket() && workerThreads > 1){
            logger.warn("Multiple worker, but not using native poller, it's useless");
            localWorkersThread = 1;
        }
        ThreadFactory threadFactory = ThreadBuilder.get()
                                                   .setDaemon(true)
                                                   .getFactory(getStringPrefix() + "/" + address.toString());
        EventLoopGroup workerGroup = poller.getEventLoopGroup(localWorkersThread, threadFactory);
        finisher = Optional.of(workerGroup::shutdownGracefully);
        bootstrap.group(workerGroup);
        addHandlers(bootstrap);
        consumer.addOptions(bootstrap);
        configureBootStrap(bootstrap);
        for (int i = 0 ; i < workerThreads ; i++) {
            ChannelFuture future = bootstrap.bind(address);
            listeningChannels.add(future);
        }
    }

    private void bindConnected(SocketAddress address) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.channelFactory(() -> poller.serverChannelProvider(transport));
        ThreadFactory threadFactoryBosses = ThreadBuilder.get()
                                                         .setDaemon(true)
                                                         .getFactory(getStringPrefix() + "/" + address + "/bosses");
        EventLoopGroup bossGroup = poller.getEventLoopGroup(getWorkerThreads(), threadFactoryBosses);
        ThreadFactory workerFactoryWorkers = ThreadBuilder.get()
                                                          .setDaemon(true)
                                                          .getFactory(getStringPrefix() + "/" + address + "/workers");
        EventLoopGroup workerGroup = poller.getEventLoopGroup(getWorkerThreads(), workerFactoryWorkers);
        bootstrap.group(bossGroup, workerGroup);
        finisher = Optional.of(() -> {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        });
        addHandlers(bootstrap);
        consumer.addOptions(bootstrap);
        configureServerBootStrap(bootstrap);
        ChannelFuture cf = bootstrap.bind(address);
        listeningChannels.add(cf);
    }

    private String getStringPrefix() {
        return threadPrefix != null ? threadPrefix : transport.toString();
    }

    protected void configureServerBootStrap(ServerBootstrap bootstrap) {
    }

    protected void configureBootStrap(Bootstrap bootstrap) {
    }

    protected abstract SA resolveAddress();
    public abstract ConnectionContext<SA> getNewConnectionContext(ChannelHandlerContext ctx, M message);

    private <C extends Channel> ChannelInitializer<C> getChannelInitializer(boolean client) {
        return new ChannelInitializer<>() {
            @Override
            public void initChannel(Channel ch) {
                NettyTransport.this.initChannel(ch, client);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                if (Helpers.isFatal(cause)) {
                    consumer.logFatalException(cause);
                    Start.fatalException(cause);
                } else {
                    consumer.exception(ctx, cause);
                }
            }
        };
    }

    protected void initChannel(Channel ch, boolean client) {
        consumer.addHandlers(ch.pipeline());
        if (ch.pipeline().get(ERROR_HANDLER_NAME) == null) {
            ch.pipeline().addLast(ERROR_HANDLER_NAME, new ErrorHandler(logger));
        }
    }

    private void addHandlers(Bootstrap bootstrap) {
        bootstrap.handler(getChannelInitializer(true));
    }

    protected void addHandlers(ServerBootstrap bootstrap) {
        bootstrap.childHandler(getChannelInitializer(false));
    }

    public synchronized void close() {
        listeningChannels.stream().map(ChannelFuture::channel).forEach(Channel::close);
        listeningChannels.clear();
        finisher.ifPresent(Runnable::run);
    }

    public void waitClose()  {
        try {
            finished.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error(() -> new FormattedMessage("Failed to stop transport: {}", Helpers.resolveThrowableException(e.getCause())), e.getCause());
        }
    }

    public static ChannelConsumer resolveConsumer(Object o) {
        if (o instanceof ChannelConsumer) {
            return (ChannelConsumer) o;
        } else if (o instanceof ConsumerProvider) {
            ConsumerProvider cp = (ConsumerProvider ) o;
            return cp.getConsumer();
        } else {
            return null;
        }
    }

}
