package loghub.senders;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import javax.net.ssl.SSLContext;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.httpclient.AbstractHttpClientService;
import lombok.Setter;

public abstract class AbstractHttpSender extends Sender {
    public abstract static class Builder<S extends AbstractHttpSender> extends Sender.Builder<S> {
        @Setter
        public String protocol = "http";
        @Setter
        private String user = null;
        @Setter
        private String password = null;
        @Setter
        private int timeout = 2;
        @Setter
        private int port = -1;
        @Setter
        private String[] destinations;
        @Setter
        private int workers;
        @Setter
        private String clientService;
        @Setter
        protected boolean withSSL = false;
        @Setter
        protected String sslKeyAlias;
        @Setter
        protected SSLContext sslContext;
    }

    protected final AbstractHttpClientService httpClient;
    protected final URI[] endpoints;

    protected AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        try {
            Class<? extends AbstractHttpClientService> httpClientClass = (Class<? extends AbstractHttpClientService>) this.getClass().getClassLoader().loadClass(
                    builder.clientService);
            AbstractHttpClientService.Builder<?> clientBuilder = (AbstractHttpClientService.Builder<?>) AbstractBuilder.resolve(httpClientClass);
            endpoints = Helpers.stringsToUri(builder.destinations, -1, "https", logger);
            clientBuilder.setPort(builder.port);
            clientBuilder.setTimeout(builder.timeout);
            clientBuilder.setUser(builder.user);
            clientBuilder.setPassword(builder.password);
            clientBuilder.setWorkers(builder.workers);
            if (builder.withSSL) {
                clientBuilder.setWithSSL(true);
                clientBuilder.setSslContext(builder.sslContext);
                clientBuilder.setSslKeyAlias(builder.sslKeyAlias);
            }

            httpClient = httpClientClass.getConstructor(AbstractHttpSender.class, AbstractHttpSender.Builder.class).newInstance(this, builder);
        } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void customStopSending() {
        httpClient.customStopSending();
    }

}
