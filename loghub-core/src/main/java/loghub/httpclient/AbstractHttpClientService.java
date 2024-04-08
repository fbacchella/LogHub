package loghub.httpclient;

import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import javax.management.ObjectName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.Helpers;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractHttpClientService {

    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_XML = "application/xml";
    public static final String TEXT_HTML = "text/html";
    public static final String TEXT_PLAIN = "text/plain";

    @Setter @Getter
    public abstract static class Builder<B extends AbstractHttpClientService> extends AbstractBuilder<B> {
        protected String password = null;
        protected String user = null;
        protected int timeout = 2;
        protected int workers;
        protected ObjectName jmxParent = null;
        protected String sslKeyAlias;
        protected SSLContext sslContext;
        protected SSLParameters sslParams;
    }

    protected final int timeout;
    protected final Logger logger;
    protected final String user;
    protected final String password;

    protected AbstractHttpClientService(AbstractHttpClientService.Builder<? extends AbstractHttpClientService> builder) {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        timeout = builder.timeout;
        user = builder.user;
        password = builder.password;
    }

    protected SSLContext resolveSslContext(AbstractHttpClientService.Builder<? extends AbstractHttpClientService> builder) {
        return Optional.ofNullable(builder.sslContext).orElseGet(this::getDefaultSslContext);
    }

    protected SSLParameters resolveSslParams(AbstractHttpClientService.Builder<? extends AbstractHttpClientService> builder,
            SSLContext sslContext) {
        return Optional.ofNullable(builder.sslParams).orElseGet(sslContext::getDefaultSSLParameters);
    }

    private SSLContext getDefaultSslContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SSL context invalid", ex);
        }
    }

    public abstract <T> HttpRequest<T> getRequest();

    public abstract <T> HttpResponse<T> doRequest(HttpRequest<T> request);

    public void customStopSending() {
        // Nothing to do
    }

}
