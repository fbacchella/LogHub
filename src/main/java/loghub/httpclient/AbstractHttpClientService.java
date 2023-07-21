package loghub.httpclient;

import javax.management.ObjectName;
import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.Helpers;
import lombok.Setter;

public abstract class AbstractHttpClientService {

    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_XML = "application/xml";
    public static final String TEXT_HTML = "text/html";
    public static final String TEXT_PLAIN = "text/plain";

    public abstract static class Builder<B extends AbstractHttpClientService> extends AbstractBuilder<B> {
        @Setter
        protected String protocol = "http";
        @Setter
        protected String password = null;
        @Setter
        protected String user = null;
        @Setter
        protected int timeout = 2;
        @Setter
        protected int port = -1;
        @Setter
        protected int workers;
        @Setter
        protected ObjectName jmxParent = null;
        @Setter
        protected boolean withSSL = false;
        @Setter
        protected String sslKeyAlias;
        @Setter
        protected SSLContext sslContext;
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

    public abstract HttpRequest getRequest();

    public abstract HttpResponse doRequest(HttpRequest request);

    public void customStopSending() {
        // Nothing to do
    }

}
