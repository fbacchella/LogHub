package loghub.netty.transport;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
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

public abstract class NettyTransport<SA extends SocketAddress, M> {

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static final AttributeKey<Principal> PRINCIPALATTRIBUTE = AttributeKey.newInstance(Principal.class.getName());

    public static final int DEFINEDSSLALIAS=-2;

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

    protected final POLLER poller;
    protected final TRANSPORT transport;
    protected final Logger logger;
    @Getter
    protected Runnable finisher;
    protected Set<ChannelFuture> listeningChannels;

    NettyTransport(POLLER poller, TRANSPORT transport) {
        this.poller = poller;
        this.transport = transport;
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
    }

    public void connect(TransportConfig config) throws InterruptedException {
        SocketAddress address = resolveAddress(config);
        if (address == null) {
            throw new IllegalArgumentException("Can't get listening address: " + "connect");
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channelFactory(() -> poller.clientChannelProvider(transport));
        ThreadFactory threadFactory = ThreadBuilder.get()
                                              .setDaemon(true)
                                              .getFactory(getStringPrefix(config) + "/" + address);
        EventLoopGroup workerGroup = poller.getEventLoopGroup(1, threadFactory);
        finisher = workerGroup::shutdownGracefully;
        config.consumer.addOptions(bootstrap);
        configureBootStrap(bootstrap, config);
        ChannelFuture cf = bootstrap.connect(address);
        cf.await();
        listeningChannels = new HashSet<>(Collections.singleton(cf));
        logger.debug("Connected to {}", address);
    }

    public void bind(TransportConfig config) throws InterruptedException {
        SocketAddress address = resolveAddress(config);
        if (address == null) {
            throw new IllegalArgumentException("Can't get listening address: " + "listen");
        }
        if (transport.isConnectedServer()) {
            bindConnected(address, config);
        } else {
            bindConnectionless(address, config);
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
        logger.debug("Bond to {}", address);
    }

    private void bindConnectionless(SocketAddress address, TransportConfig config) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channelFactory(() -> poller.clientChannelProvider(transport));
        if (config.bufferSize > 0) {
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(config.bufferSize));
        }
        // Needed because Netty's UDP is not multi-thread, see http://marrachem.blogspot.fr/2014/09/multi-threaded-udp-server-with-netty-on.html
        if ((poller == POLLER.EPOLL || poller == POLLER.KQUEUE) && config.workerThreads > 1) {
            bootstrap.option(UnixChannelOption.SO_REUSEPORT, true);
        } else if (poller != POLLER.EPOLL && poller != POLLER.KQUEUE && config.workerThreads > 1){
            logger.warn("Multiple worker, but not using native poller, it's useless");
            config.workerThreads = 1;
        }
        ThreadFactory threadFactory = ThreadBuilder.get()
                                                   .setDaemon(true)
                                                   .getFactory(getStringPrefix(config) + "/" + address.toString());
        EventLoopGroup workerGroup = poller.getEventLoopGroup(config.workerThreads, threadFactory);
        finisher = workerGroup::shutdownGracefully;
        bootstrap.group(workerGroup);
        addHandlers(bootstrap, config);
        config.consumer.addOptions(bootstrap);
        configureBootStrap(bootstrap, config);
        listeningChannels = new HashSet<>(config.workerThreads);
        for (int i = 0 ; i < config.workerThreads ; i++) {
            ChannelFuture future = bootstrap.bind(address);
            listeningChannels.add(future);
        }
    }

    private void bindConnected(SocketAddress address, TransportConfig config) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.channelFactory(() -> poller.serverChannelProvider(transport));
        ThreadFactory threadFactoryBosses = ThreadBuilder.get()
                                                         .setDaemon(true)
                                                         .getFactory(getStringPrefix(config) + "/" + address + "/bosses");
        EventLoopGroup bossGroup = poller.getEventLoopGroup(config.getWorkerThreads(), threadFactoryBosses);
        ThreadFactory workerFactoryWorkers = ThreadBuilder.get()
                                                          .setDaemon(true)
                                                          .getFactory(getStringPrefix(config) + "/" + address + "/workers");
        EventLoopGroup workerGroup = poller.getEventLoopGroup(config.getWorkerThreads(), workerFactoryWorkers);
        bootstrap.group(bossGroup, workerGroup);
        finisher = () -> {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        };
        addHandlers(bootstrap, config);
        config.consumer.addOptions(bootstrap);
        configureServerBootStrap(bootstrap, config);
        ChannelFuture cf = bootstrap.bind(address);
        listeningChannels = new HashSet<>(Collections.singleton(cf));
    }

    private String getStringPrefix(TransportConfig config) {
        return config.threadPrefix != null ? config.threadPrefix : transport.toString();
    }

    protected void configureServerBootStrap(ServerBootstrap bootstrap, TransportConfig config) {
    }

    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
    }

    protected abstract SA resolveAddress(TransportConfig config);
    public abstract ConnectionContext<SA> getNewConnectionContext(ChannelHandlerContext ctx, M message);

    private void addHandlers(Bootstrap bootstrap, TransportConfig config) {
        ChannelConsumer consumer = config.consumer;
        ChannelHandler handler = new ChannelInitializer<>() {
            @Override
            public void initChannel(Channel ch) {
                consumer.addHandlers(ch.pipeline());
                NettyTransport.this.addErrorHandler(ch.pipeline());
            }
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                if (Helpers.isFatal(cause)) {
                    consumer.logFatalException(cause);
                    Start.fatalException(cause);
                } else {
                    consumer.exception(ctx, cause);
                }
            }
        };
        bootstrap.handler(handler);
    }

    protected void addHandlers(ServerBootstrap bootstrap, TransportConfig config) {
        ChannelConsumer consumer = config.consumer;
        ChannelHandler handler = new ChannelInitializer<>() {
            @Override
            public void initChannel(Channel ch) {
                consumer.addHandlers(ch.pipeline());
                if (NettyTransport.this instanceof IpServices && config.withSsl) {
                    ((IpServices)NettyTransport.this).addSslHandler(config, ch.pipeline(), logger);
                }
                NettyTransport.this.addErrorHandler(ch.pipeline());
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
        bootstrap.childHandler(handler);
    }

    protected void addErrorHandler(ChannelPipeline p) {
        p.addLast("errorhandler", new ErrorHandler(logger));
    }

    public void close() {
        finisher.run();
        synchronized (listeningChannels) {
            listeningChannels.stream().map(ChannelFuture::channel).forEach(Channel::close);
        }
    }

    public void waitClose()  {
        for (ChannelFuture cf: listeningChannels) {
            try {
                cf.channel().closeFuture().await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        synchronized (listeningChannels) {
            listeningChannels.clear();
        }
    }

    @SuppressWarnings("unchecked")
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
