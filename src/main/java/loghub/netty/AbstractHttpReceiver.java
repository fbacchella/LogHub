package loghub.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import loghub.configuration.Properties;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.AccessControl;
import loghub.netty.http.HttpRequestProcessing;
import loghub.receivers.Blocking;

@Blocking(true)
public abstract class AbstractHttpReceiver extends AbstractTcpReceiver<AbstractHttpReceiver, AbstractHttpReceiver.HttpReceiverServer, AbstractHttpReceiver.HttpReceiverServer.Builder, HttpMessage> {

    protected static class HttpReceiverServer extends AbstractHttpServer<HttpReceiverServer, HttpReceiverServer.Builder> {

        public static class Builder extends AbstractHttpServer.Builder<HttpReceiverServer, Builder> {
            HttpRequestProcessing requestProcessor;
            AbstractHttpReceiver receiver;
            public Builder setReceiveHandler(HttpRequestProcessing requestProcessor) {
                this.requestProcessor = requestProcessor;
                return this;
            }
            public Builder setReceiver(AbstractHttpReceiver receiver) {
                this.receiver = receiver;
                return this;
            }
            @Override
            public HttpReceiverServer build() throws IllegalArgumentException, InterruptedException {
                return new HttpReceiverServer(this);
            }
        }

        protected final HttpRequestProcessing requestProcessor;
        protected final ContextExtractor<HttpMessage> resolver;
        protected HttpReceiverServer(Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
            this.requestProcessor = builder.requestProcessor;
            this.resolver = new ContextExtractor<HttpMessage>(HttpMessage.class, builder.receiver);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            if (getAuthHandler() != null) {
                p.addLast("Authentication", new AccessControl(getAuthHandler()));
                logger.debug("Added authentication");
            }
            p.addBefore("HttpObjectAggregator", ContextExtractor.NAME, resolver);
            p.addLast("RequestProcessor", requestProcessor);
        }

    }

    public static abstract class Builder<B extends AbstractHttpReceiver> extends AbstractTcpReceiver.Builder<B> {
    };

    protected AbstractHttpReceiver(Builder<? extends AbstractHttpReceiver>  builder) {
        super(builder);
    }

    @Override
    protected HttpReceiverServer.Builder getServerBuilder() {
        return new HttpReceiverServer.Builder().setReceiver(this);
    }

    @Override
    public boolean configure(Properties properties, HttpReceiverServer.Builder builder) {
        settings(builder);
        return super.configure(properties, builder);
    }

    protected void settings(loghub.netty.AbstractHttpReceiver.HttpReceiverServer.Builder builder) {
        // Empty
    }

    @Override
    public ByteBuf getContent(HttpMessage message) {
        throw new UnsupportedOperationException();
    }

}