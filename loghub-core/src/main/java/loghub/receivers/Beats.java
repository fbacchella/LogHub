package loghub.receivers;

import java.util.List;

import org.apache.logging.log4j.Level;
import org.logstash.beats.AckEncoder;
import org.logstash.beats.Batch;
import org.logstash.beats.BeatsHandler;
import org.logstash.beats.BeatsParser;
import org.logstash.beats.ConnectionHandler;
import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import com.codahale.metrics.Histogram;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.jackson.JacksonBuilder;
import loghub.metrics.CustomStats;
import loghub.metrics.Stats;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ChannelConsumer;
import loghub.netty.CloseOnError;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyReceiver;
import loghub.netty.transport.TRANSPORT;
import lombok.Getter;
import lombok.Setter;

@SelfDecoder
@CloseOnError
@BuilderClass(Beats.Builder.class)
@Blocking
public class Beats extends NettyReceiver<Beats, ByteBuf, Beats.Builder> implements ConsumerProvider, CustomStats {

    @Setter
    public static class Builder extends NettyReceiver.Builder<Beats, ByteBuf, Beats.Builder> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        private int clientInactivityTimeoutSeconds;
        private int maxPayloadSize = 8192;
        private int workers = 4;
        @Override
        public Beats build() {
            return new Beats(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Sharable
    private class StatsHandler extends MessageToMessageDecoder<Batch> {

        @Override
        protected void decode(ChannelHandlerContext ctx, Batch msg, List<Object> out) {
            Stats.getMetric(Beats.this, "batchesSize", Histogram.class).update(msg.size());
            out.add(msg);
        }

    }

    @Sharable
    private class BeatsErrorHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                                    Object msg) {
            logger.warn("Not processed message {}", msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Forward non HTTP error
            if (cause instanceof DecoderException) {
                Beats.this.manageDecodeException(new DecodeException("Invalid beats message", cause.getCause() != null ? cause.getCause() : cause));
            } else {
                ctx.fireExceptionCaught(cause);
            }
        }
    }

    private final IMessageListener messageListener;
    private final EventExecutorGroup idleExecutorGroup;
    private final EventExecutorGroup beatsHandlerExecutorGroup;
    private final ObjectReader reader;

    @Getter
    private final int clientInactivityTimeoutSeconds;
    @Getter
    private final int maxPayloadSize;
    @Getter
    private final int workers;

    public Beats(Builder builder) {
        super(builder);
        this.clientInactivityTimeoutSeconds = builder.clientInactivityTimeoutSeconds;
        this.maxPayloadSize = builder.maxPayloadSize;
        this.idleExecutorGroup = new DefaultEventExecutorGroup(builder.workers, ThreadBuilder.get().setDaemon(true).getFactory(getReceiverName() + "/idle"));
        this.beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(builder.workers, ThreadBuilder.get().setDaemon(true).getFactory(getReceiverName() + "/beatsHandler"));
        this.workers = builder.workers;
        this.reader = JacksonBuilder.get(JsonMapper.class)
                                    .getReader();

        this.messageListener = new IMessageListener() {

            @Override
            public void onChannelInitializeException(ChannelHandlerContext arg0, Throwable error) {
                logger.fatal("Beats initialization exception: {}", () -> Helpers.resolveThrowableException(error));
                logger.catching(Level.DEBUG, error);
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
            public void onNewMessage(ChannelHandlerContext ctx, Message beatsMessage) {
                logger.trace("new beats message {}", beatsMessage::getData);
                ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
                Event newEvent = getEventsFactory().newEvent(cctx);
                beatsMessage.getData().forEach((i, j) -> {
                    String key = i;
                    if (key.startsWith("@")) {
                        key = "_" + key.substring(1);
                    }
                    newEvent.put(key, j);
                });
                Stats.newReceivedEvent(Beats.this);
                ctx.fireChannelRead(newEvent);
            }
        };
    }

    @Override
    public void registerCustomStats() {
        Stats.register(this, "batchesSize", Histogram.class);
    }

    @Override
    protected String getThreadPrefix(Beats.Builder builder) {
        return "BeatsReceiver";
    }

    @Override
    public ChannelConsumer getConsumer() {
        StatsHandler statsHandler = new StatsHandler();
        BeatsErrorHandler errorHandler = new BeatsErrorHandler();

        return new BaseChannelConsumer<>(this) {
            @Override
            public void addHandlers(ChannelPipeline pipe) {
                super.addHandlers(pipe);
                // From org.logstash.beats.Server
                // We have set a specific executor for the idle check, because the `beatsHandler` can be
                // blocked on the queue, this the idleStateHandler manage the `KeepAlive` signal.
                pipe.addBefore(idleExecutorGroup, "Sender", "KeepAlive", new IdleStateHandler(clientInactivityTimeoutSeconds, 5, 0));
                pipe.addBefore("Sender", "Acker", new AckEncoder());
                pipe.addBefore("Sender", "ConnectionHandler", new ConnectionHandler());
                pipe.addBefore(beatsHandlerExecutorGroup, "Sender", "BeatsSplitter", new BeatsParser(maxPayloadSize, reader));
                pipe.addBefore(beatsHandlerExecutorGroup, "Sender", "BeatsStats", statsHandler);
                pipe.addBefore(beatsHandlerExecutorGroup, "Sender", "BeatsHandler", new BeatsHandler(messageListener));
                pipe.addAfter("Sender", "BeatsErrorHandler", errorHandler);
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
        return "BeatsReceiver/" + Helpers.ListenString(getListen()) + "/" + getPort();
    }

    @Override
    public void close() {
        try {
            idleExecutorGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            beatsHandlerExecutorGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        Stats.newReceivedMessage(this, message.readableBytes());
        return message;
    }

}
