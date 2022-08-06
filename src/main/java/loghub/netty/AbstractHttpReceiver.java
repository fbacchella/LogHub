package loghub.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import loghub.netty.transport.TRANSPORT;
import loghub.receivers.Blocking;

@Blocking
public abstract class AbstractHttpReceiver<R extends AbstractHttpReceiver<R>> extends NettyReceiver<R, HttpMessage> implements ConsumerProvider {

    public abstract static class Builder<R extends AbstractHttpReceiver<R>> extends NettyReceiver.Builder<R, HttpMessage> {
        protected Builder() {
            setTransport(TRANSPORT.TCP);
        }
    }

    protected final ContextExtractor<R, HttpMessage> resolver;

    protected AbstractHttpReceiver(Builder<R> builder) {
        super(builder);
        this.resolver = new ContextExtractor<>(HttpMessage.class, this);
    }

    @Override
    public ChannelConsumer getConsumer() {
        HttpChannelConsumer.Builder builder = HttpChannelConsumer.getBuilder();
        builder.setLogger(logger)
               .setAuthHandler(config.getAuthHandler())
               .setModelSetup(this::configureModel);
        configureConsumer(builder);
        return builder.build();
    }

    protected void configureConsumer(HttpChannelConsumer.Builder builder) {
        // default does nothing
    }

    private void configureModel(ChannelPipeline pipeline) {
        pipeline.addBefore("HttpObjectAggregator", ContextExtractor.NAME, resolver);
        modelSetup(pipeline);
    }

    protected abstract void modelSetup(ChannelPipeline pipeline);

    @Override
    public ByteBuf getContent(HttpMessage message) {
        throw new UnsupportedOperationException();
    }

}
