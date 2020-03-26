package loghub.netty;

import io.netty.channel.ChannelPipeline;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.AccessControl;
import loghub.netty.http.HttpRequestProcessing;
import loghub.receivers.Blocking;

@Blocking(true)
public abstract class AbstractHttp extends AbstractTcpReceiver<AbstractHttp, AbstractHttp.HttpReceiverServer, AbstractHttp.HttpReceiverServer.Builder> {

    protected static class HttpReceiverServer extends AbstractHttpServer<HttpReceiverServer, HttpReceiverServer.Builder> {

        public static class Builder extends AbstractHttpServer.Builder<HttpReceiverServer, Builder> {
            HttpRequestProcessing receiver;
            public Builder setReceiveHandler(HttpRequestProcessing recepter) {
                this.receiver = recepter;
                return this;
            }
            @Override
            public HttpReceiverServer build() throws IllegalArgumentException, InterruptedException {
                return new HttpReceiverServer(this);
            }
        }

        final HttpRequestProcessing receiver;
        protected HttpReceiverServer(Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
            this.receiver = builder.receiver;
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            if (getAuthHandler() != null) {
                p.addLast("authentication", new AccessControl(getAuthHandler()));
                logger.debug("Added authentication");
            }
            p.addLast("receiver", receiver);
        }

    }

    public static abstract class Builder<B extends AbstractHttp> extends AbstractTcpReceiver.Builder<B> {
    };

    protected AbstractHttp(Builder<? extends AbstractHttp>  builder) {
        super(builder);
    }

    @Override
    protected HttpReceiverServer.Builder getServerBuilder() {
        return new HttpReceiverServer.Builder();
    }

    protected void settings(loghub.netty.AbstractHttp.HttpReceiverServer.Builder builder) {
        
    }

}
