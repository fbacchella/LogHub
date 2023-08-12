package loghub;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import loghub.netty.ChannelConsumer;
import loghub.netty.transport.UnixDgramTransport;

/**
 * A java implementation of <a href="https://www.freedesktop.org/software/systemd/man/sd_notify.html">sd_notify</a>
 */
public abstract class SystemdHandler {

    private static final Logger logger = LogManager.getLogger();

    public static SystemdHandler nope() {
        return new SystemdHandlerInactive();
    }

    public static SystemdHandler start() {
        String notifySocket = System.getenv("NOTIFY_SOCKET");
        if (notifySocket != null && Files.isWritable(Paths.get(notifySocket))) {
            try {
                return new SystemdHandlerActive(notifySocket);
            } catch (InterruptedException e) {
                logger.error("Starting of systemd handler interrupted");
                Thread.currentThread().interrupt();
                return new SystemdHandlerInactive();
            }
        } else {
            return new SystemdHandlerInactive();
        }
    }

    private static class SystemdEncoder extends MessageToByteEncoder<String> {
        @Override
        protected void encode(ChannelHandlerContext channelHandlerContext, String s, ByteBuf byteBuf) {
            byteBuf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class SystemdHandlerActive extends SystemdHandler implements ChannelConsumer {

        private final Channel notifySocketChannel;

        private SystemdHandlerActive(String notifySocketPath) throws InterruptedException {
            UnixDgramTransport.Builder builder = UnixDgramTransport.getBuilder();
            builder.setEndpoint(notifySocketPath);
            builder.setConsumer(this);
            UnixDgramTransport notifySocket = builder.build();
            notifySocketChannel = notifySocket.connect().sync().channel();
            notifySocketChannel.writeAndFlush("STATUS=Starting\n");
        }

        @Override
        public void addHandlers(ChannelPipeline pipe) {
            pipe.addFirst(new SystemdEncoder());
        }

        @Override
        public void exception(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception with sytemd handler: " + Helpers.resolveThrowableException(cause));
        }

        @Override
        public void logFatalException(Throwable cause) {
            logger.error("Exception with sytemd handler: " + Helpers.resolveThrowableException(cause));
        }

        @Override
        public void setStatus(String message) {
            notifySocketChannel.writeAndFlush(String.format("STATUS=%s\n", message));
        }

        @Override
        public void started() {
            notifySocketChannel.writeAndFlush("READY=1\n");
        }

        @Override
        public void stopping() {
            notifySocketChannel.writeAndFlush("STOPPING=1\n");
        }
    }

    private static class SystemdHandlerInactive extends SystemdHandler {

        @Override
        public void setStatus(String message) {
            // Do nothing
        }

        @Override
        public void started() {
            // Do nothing
        }

        @Override
        public void stopping() {
            // Do nothing
        }
    }

    public abstract void setStatus(String message);

    public abstract void started();

    public abstract void stopping();

}
