package loghub.senders;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import com.codahale.metrics.Meter;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.httpclient.AbstractHttpClientService;
import loghub.metrics.Stats;
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

    /**
     * Prefix used to build the name of the per-status HTTP counter registered through {@link Stats}.
     */
    private static final String HTTPSTATUS_METRIC_PREFIX = "HTTPStatus.";
    protected final AbstractHttpClientService httpClient;
    protected final URI[] endpoints;

    protected AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        try {
            @SuppressWarnings("unchecked")
            Class<AbstractHttpClientService> clientClass = (Class<AbstractHttpClientService>) getClassLoader().loadClass(builder.clientService);
            AbstractHttpClientService.Builder<?> clientBuilder = (AbstractHttpClientService.Builder<?>) AbstractBuilder.resolve(clientClass);
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

    /**
     * In addition to the default sender metrics, pre-registers the counters for the HTTP statuses expected
     * in the context of an HTTP sender, so they are always defined
     * (reading as 0) instead of only appearing once first encountered.
     */
    @Override
    public void registerCustomStats() {
        super.registerCustomStats();

        /*
         * The HTTP statuses expected in the context of a generic HTTP sender, whose counter is always defined
         * (even at 0) as soon as the sender is configured, instead of only appearing once first encountered.
         */
        List<Integer> commonHttpStatus = List.of(200, 400, 401, 403, 404, 409, 413, 429, 500, 503);
        commonHttpStatus.forEach(status -> Stats.getMetric(this, HTTPSTATUS_METRIC_PREFIX + status, Meter.class));
    }

    /**
     * Counts an HTTP request made by this sender, broken down by the returned HTTP status, so the whole
     * status distribution (successes and failures alike) remains visible. This is a counter specific to
     * {@link AbstractHttpSender}, built on top of the generic {@link Stats} API instead of extending it.
     * @param status the HTTP status returned by the server
     */
    protected void countHttpStatus(int status) {
        Stats.getMetric(this, HTTPSTATUS_METRIC_PREFIX + status, Meter.class).mark();
    }

    /**
     * Returns the number of HTTP requests that returned the given status, for monitoring and
     * testing purposes.
     * @param status the HTTP status to look up
     * @return the number of requests that returned {@code status}
     */
    long getHttpStatusCount(int status) {
        return Stats.getMetric(this, HTTPSTATUS_METRIC_PREFIX + status, Meter.class).getCount();
    }

}
