package loghub.senders;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ApacheHttpClientService;
import lombok.Setter;

public abstract class AbstractHttpSender extends Sender {
    public abstract static class Builder<S extends AbstractHttpSender> extends Sender.Builder<S> {
        @Setter
        public String protocol = "https";
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
        private String clientService = ApacheHttpClientService.class.getName();
        @Setter
        protected String sslKeyAlias;
        @Setter
        protected SSLContext sslContext;
        @Setter
        protected SSLParameters sslParams;
    }

    protected final AbstractHttpClientService httpClient;
    protected final URI[] endpoints;

    protected AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        try {
            @SuppressWarnings("unchecked")
            Class<AbstractHttpClientService> clientClass = (Class<AbstractHttpClientService>) getClass().getClassLoader().loadClass(builder.clientService);
            AbstractHttpClientService.Builder clientBuilder = (AbstractHttpClientService.Builder) AbstractBuilder.resolve(clientClass);
            endpoints = Helpers.stringsToUri(builder.destinations, builder.port, builder.protocol, logger);
            clientBuilder.setTimeout(builder.timeout);
            clientBuilder.setUser(builder.user);
            clientBuilder.setPassword(builder.password);
            clientBuilder.setWorkers(builder.workers);
            if (builder.sslContext != null) {
                clientBuilder.setSslContext(builder.sslContext);
                clientBuilder.setSslKeyAlias(builder.sslKeyAlias);
                clientBuilder.setSslParams(builder.sslParams);
            }
            httpClient = (AbstractHttpClientService) clientBuilder.build();
        } catch (InvocationTargetException | ClassNotFoundException ex) {
            throw new IllegalArgumentException("HTTP client class not usable: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    @Override
    public void customStopSending() {
        httpClient.customStopSending();
    }

}
