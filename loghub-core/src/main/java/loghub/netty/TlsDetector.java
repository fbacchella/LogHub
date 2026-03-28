package loghub.netty;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;

@Sharable
public class TlsDetector extends ChannelInboundHandlerAdapter {

    public static final String TLSDETECTOR_NAME = "TLSDetector";
    public static final String SSLHANDLER_NAME  = "TLSHandler";

    private final BiConsumer<ChannelPipeline, BiConsumer<ChannelPipeline, SslHandler>> inserter;
    private final Logger logger;
    private boolean inspected;

    public TlsDetector(Logger logger, BiConsumer<ChannelPipeline, BiConsumer<ChannelPipeline, SslHandler>> inserter) {
        this.inserter = inserter;
        this.logger = logger;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf in = (ByteBuf) msg;

        if (in.readableBytes() < 5) {
            // Not enough bytes yet; pass through and wait for more data.
            ctx.fireChannelRead(msg);
            return;
        }

        ChannelPipeline p = ctx.pipeline();

        if (SslHandler.isEncrypted(in, false)) {
            logger.debug("Detected encrypted channel {}", ctx);
            inserter.accept(p, (lp, h)-> lp.addAfter(TLSDETECTOR_NAME, SSLHANDLER_NAME, h));
        } else {
            logger.debug("Not using TLS on channel {}", ctx);
        }
        // Remove this detector, then forward the buffer to the next handler
        // (either the freshly inserted SslHandler or the next plain-text handler).
        p.remove(this);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.fireExceptionCaught(cause);
    }
}
