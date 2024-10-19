package loghub.senders;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.httpclient.AbstractHttpClientService;
import lombok.Setter;

public abstract class AbstractHttpSender extends Sender {
    @Setter
    public abstract static class Builder<S extends AbstractHttpSender> extends Sender.Builder<S> {
        public String protocol = "https";
        private String user = null;
        private String password = null;
        private int timeout = 2;
        private int port = -1;
        private String[] destinations;
        private String clientService = "loghub.httpclient.ApacheHttpClientService";
        protected String sslKeyAlias;
        protected SSLContext sslContext;
        protected SSLParameters sslParams;
    }

    protected final AbstractHttpClientService httpClient;
    protected final URI[] endpoints;

    protected AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        try {
            @SuppressWarnings("unchecked")
            Class<AbstractHttpClientService> clientClass = (Class<AbstractHttpClientService>) getClassLoader().loadClass(builder.clientService);
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
