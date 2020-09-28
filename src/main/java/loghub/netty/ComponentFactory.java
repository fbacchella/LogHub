package loghub.netty;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.ssl.NotSslRecordException;
import loghub.Helpers;
import loghub.netty.servers.AbstractNettyServer;

public abstract class ComponentFactory<BS extends AbstractBootstrap<BS,BSC>, BSC extends Channel, SA extends SocketAddress> {

    @Sharable
    private static class ErrorHandler extends SimpleChannelInboundHandler<Object> {
        private final Logger logger;
        private ErrorHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                                    Object msg)
                                                    throws Exception {
            logger.warn("Not processed message {}", msg);
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                        throws Exception {
            if (cause.getCause() instanceof NotSslRecordException) {
                logger.warn("Not a SSL connexion from {} on SSL listen", ctx.channel().remoteAddress());
            } else if (cause.getCause() instanceof SSLHandshakeException) {
                logger.warn("Failed SSL handshake from {}: {}", ctx.channel().remoteAddress(), Helpers.resolveThrowableException(cause));
                logger.catching(Level.DEBUG, cause);
            } else if (cause instanceof IOException) {
                logger.warn("IO Exception from {}: {}", ctx.channel().remoteAddress(), Helpers.resolveThrowableException(cause));
                logger.catching(Level.DEBUG, cause);
            } else {
                logger.warn("Not handled exception {} from {}", Helpers.resolveThrowableException(cause), ctx.channel().remoteAddress());
                logger.throwing(Level.WARN, cause);
            }
            if (! (ctx.channel() instanceof DatagramChannel)) {
                // UDP should not be close
                ctx.close();
                logger.warn("channel closed");
            }
        }
    }

    public abstract EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory);
    public abstract ChannelFactory<BSC> getInstance();
    public abstract BS getBootStrap();
    public abstract void group(int threads, ThreadFactory threadFactory);
    public abstract Runnable finisher();
    public abstract void addChildhandlers(ChannelConsumer<BS, BSC> source, AbstractNettyServer<?, BS, BSC, ?, SA, ?, ?> server, Logger logger);
    public void addHandlers(ChannelConsumer<BS, BSC> source, AbstractNettyServer<?, BS, BSC, ?, SA, ?, ?> server, Logger logger) { };
    protected void addErrorHandler(ChannelPipeline p, Logger logger) {
        p.addLast("errorhandler", new ErrorHandler(logger));
    }

}
