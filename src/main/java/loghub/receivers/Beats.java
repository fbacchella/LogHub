package loghub.receivers;

import java.util.concurrent.BlockingQueue;

import org.logstash.beats.AckEncoder;
import org.logstash.beats.BeatsHandler;
import org.logstash.beats.BeatsParser;
import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import loghub.Event;
import loghub.Pipeline;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.AbstractTcpReceiver;
import loghub.netty.ChannelConsumer;
import loghub.netty.CloseOnError;
import loghub.netty.ConsumerProvider;
import loghub.netty.SelfDecoder;
import loghub.netty.servers.TcpServer;
import loghub.netty.servers.TcpServer.Builder;

@SelfDecoder
@CloseOnError
public class Beats extends AbstractTcpReceiver<Beats, TcpServer, TcpServer.Builder> implements ConsumerProvider<Beats, ServerBootstrap, ServerChannel> {

    private final IMessageListener messageListener;
    private EventExecutorGroup idleExecutorGroup;
    private int clientInactivityTimeoutSeconds;
    private int maxPayloadSize = 8192;

    public Beats(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
        idleExecutorGroup = new DefaultEventExecutorGroup(4);
        messageListener = new IMessageListener() {

            @Override
            public void onChannelInitializeException(ChannelHandlerContext arg0, Throwable error) {
                logger.fatal("Beats initialization exception: {}", error.getCause());
            }

            @Override
            public void onConnectionClose(ChannelHandlerContext arg0) {
                logger.debug("onConnectionClose {}", arg0);
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable error) {
                ctx.fireExceptionCaught(error);
            }

            @Override
            public void onNewConnection(ChannelHandlerContext arg0) {
                logger.debug("onNewConnection {}", arg0);
            }

            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message arg1) {
                logger.trace("new beats message {}", arg1);
                ctx.fireChannelRead(arg1.getData());
            }
        };
    }

    @Override
    protected Builder getServerBuilder() {
        return new TcpServer.Builder();
    }

    @Override
    public ChannelConsumer<ServerBootstrap, ServerChannel> getConsumer() {
        return new BaseChannelConsumer<Beats, ServerBootstrap, ServerChannel, ByteBuf>(this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                pipe.addBefore("Sender", "Splitter", new BeatsParser(maxPayloadSize));
                // From org.logstash.beats.Server
                // We have set a specific executor for the idle check, because the `beatsHandler` can be
                // blocked on the queue, this the idleStateHandler manage the `KeepAlive` signal.
                pipe.addBefore(idleExecutorGroup, "Splitter", "KeepAlive", new IdleStateHandler(clientInactivityTimeoutSeconds, 5, 0));
                pipe.addAfter("Splitter", "Acker", new AckEncoder());
                pipe.addAfter("Acker", "Handler", new BeatsHandler(messageListener));
            }

            @Override
            public void addOptions(ServerBootstrap bootstrap) {
                // From org.logstash.beats.Server
                // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                bootstrap.childOption(ChannelOption.SO_LINGER, 0);
                super.addOptions(bootstrap);
            }
        };
    }

    @Override
    public String getReceiverName() {
        return "BeatsReceiver/" + getListenAddress();
    }

    @Override
    public void close() {
        try {
            idleExecutorGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }

    public int getClientInactivityTimeout() {
        return clientInactivityTimeoutSeconds;
    }

    public void setClientInactivityTimeout(int clientInactivityTimeoutSeconds) {
        this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
    }

    /**
     * @return the maxPayloadSize
     */
    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    /**
     * @param maxPayloadSize the maxPayloadSize to set
     */
    public void setMaxPayloadSize(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
    }

}
