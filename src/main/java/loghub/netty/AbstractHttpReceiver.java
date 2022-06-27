package loghub.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpServerCodec;
import loghub.netty.http.HttpRequestProcessing;
import loghub.receivers.Blocking;
import loghub.receivers.Journald;
import lombok.Setter;

@Blocking(true)
public abstract class AbstractHttpReceiver extends NettyReceiver<HttpMessage> implements ConsumerProvider {

    protected static class HttpReceiverChannelConsumer extends HttpChannelConsumer<HttpReceiverChannelConsumer> {

        public static class Builder extends HttpChannelConsumer.Builder<HttpReceiverChannelConsumer> {
            @Setter
            HttpRequestProcessing requestProcessor;
            @Setter
            AbstractHttpReceiver receiver;
            public HttpReceiverChannelConsumer build() {
                return new HttpReceiverChannelConsumer(this);
            }
        }
        public static HttpReceiverChannelConsumer.Builder getBuilder() {
            return new HttpReceiverChannelConsumer.Builder();
        }

        protected final HttpRequestProcessing requestProcessor;
        protected final ContextExtractor<HttpMessage> resolver;

        protected HttpReceiverChannelConsumer(Builder builder) {
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

    public abstract static class Builder<B extends AbstractHttpReceiver> extends NettyReceiver.Builder<B> {
    }

    protected AbstractHttpReceiver(Builder<? extends AbstractHttpReceiver>  builder) {
        super(builder);
    }

    @Override
    public ChannelConsumer getConsumer() {
        HttpReceiverChannelConsumer.Builder builder = HttpReceiverChannelConsumer.getBuilder();
        builder.setLogger(logger);
        builder.setAuthHandler(config.getAuthHandler());
        configureConsumer(builder);
        builder.setReceiver(this);
        return builder.build();
    }

    protected abstract void configureConsumer(HttpReceiverChannelConsumer.Builder builder);

    @Override
    public ByteBuf getContent(HttpMessage message) {
        throw new UnsupportedOperationException();
    }

}
