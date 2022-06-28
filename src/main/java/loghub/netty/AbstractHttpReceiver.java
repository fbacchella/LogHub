package loghub.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import loghub.netty.http.HttpRequestProcessing;
import loghub.receivers.Blocking;
import lombok.Setter;

@Blocking
public abstract class AbstractHttpReceiver<R extends AbstractHttpReceiver<R>> extends NettyReceiver<R, HttpMessage> implements ConsumerProvider {

    protected static class HttpReceiverChannelConsumer<R extends AbstractHttpReceiver<R>> extends HttpChannelConsumer<HttpReceiverChannelConsumer<R>> {

        public static class Builder<R extends AbstractHttpReceiver<R>> extends HttpChannelConsumer.Builder<HttpReceiverChannelConsumer<R>> {
            @Setter
            HttpRequestProcessing requestProcessor;
            @Setter
            AbstractHttpReceiver<R> receiver;
            public HttpReceiverChannelConsumer<R> build() {
                return new HttpReceiverChannelConsumer<>(this);
            }
        }
        public static <R extends AbstractHttpReceiver<R>> HttpReceiverChannelConsumer.Builder<R> getBuilder() {
            return new HttpReceiverChannelConsumer.Builder<>();
        }

        protected final HttpRequestProcessing requestProcessor;
        protected final ContextExtractor<R, HttpMessage> resolver;

        @SuppressWarnings("unchecked")
        protected HttpReceiverChannelConsumer(Builder<R> builder) {
            super(builder);
            this.requestProcessor = builder.requestProcessor;
            this.resolver = new ContextExtractor<>(HttpMessage.class, builder.receiver);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addBefore("HttpObjectAggregator", ContextExtractor.NAME, resolver);
            p.addLast("RequestProcessor", requestProcessor);
        }

    }

    public abstract static class Builder<R extends AbstractHttpReceiver<R>> extends NettyReceiver.Builder<R, HttpMessage> {
    }

    protected AbstractHttpReceiver(Builder<R>  builder) {
        super(builder);
    }

    @Override
    public ChannelConsumer getConsumer() {
        HttpReceiverChannelConsumer.Builder<R> builder = HttpReceiverChannelConsumer.getBuilder();
        builder.setLogger(logger);
        builder.setAuthHandler(config.getAuthHandler());
        configureConsumer(builder);
        builder.setReceiver(this);
        return builder.build();
    }

    protected abstract void configureConsumer(HttpReceiverChannelConsumer.Builder<R> builder);

    @Override
    public ByteBuf getContent(HttpMessage message) {
        throw new UnsupportedOperationException();
    }

}
