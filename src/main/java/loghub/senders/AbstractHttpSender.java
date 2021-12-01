package loghub.senders;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hc.client5.http.HttpHostConnectException;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.IOCallback;
import org.apache.hc.core5.pool.ConnPoolControl;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.VersionInfo;
import org.apache.logging.log4j.Level;

import loghub.Helpers;
import loghub.configuration.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class AbstractHttpSender extends Sender {

    public abstract static class Builder<S extends AbstractHttpSender> extends Sender.Builder<S> {
        @Setter
        private String protocol = "http";
        @Setter
        private String password = null;
        @Setter
        private String login = null;;
        @Setter
        private String user = null;
        @Setter
        private int timeout = 2;
        @Setter
        private int port = -1;
        @Setter
        private String[] destinations;
    };

    @Accessors(fluent = false, chain = true)
    protected static class HttpRequest {
        @Getter @Setter
        private String verb = "GET";
        private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
        @Getter @Setter
        private URL url;
        private final List<BasicHeader> headers = new ArrayList<>();
        private HttpEntity content;

        public String getHttpVersion() {
            return httpVersion.toString();
        }
        public HttpRequest setHttpVersion(int major, int minor) {
            this.httpVersion = HttpVersion.get(major, minor);
            return this;
        }
        public HttpRequest addHeader(String header, String value) {
            headers.add(new BasicHeader(header, value));
            return this;
        }
        public HttpRequest clearHeaders() {
            headers.clear();
            return this;
        }
        public HttpRequest setTypeAndContent(ContentType mimeType, byte[] content) throws IOException {
            this.content = HttpEntities.createGzipped(content, mimeType.realType);
            return this;
        }
        public HttpRequest setTypeAndContent(ContentType mimeType, ContentWriter source) throws IOException {
            IOCallback<OutputStream> cp = source::writeTo;
            content = HttpEntities.createGzipped(cp, mimeType.realType);
            return this;
        }
    }

    protected static interface ContentWriter {
        void writeTo(OutputStream outStream) throws IOException;
    }

    protected enum ContentType {

        APPLICATION_OCTET_STREAM(org.apache.hc.core5.http.ContentType.APPLICATION_OCTET_STREAM),
        APPLICATION_JSON(org.apache.hc.core5.http.ContentType.APPLICATION_JSON),
        TEXT_XML(org.apache.hc.core5.http.ContentType.TEXT_XML);

        private final org.apache.hc.core5.http.ContentType realType;

        private ContentType(org.apache.hc.core5.http.ContentType realType) {
            this.realType = realType;
        }
        
        public Charset getCharset() {
            return realType.getCharset();
        }

        @Override
        public String toString() {
            return realType.toString();
        }

    };

    protected class HttpResponse implements Closeable {
        private final HttpHost host;
        private final Optional<CloseableHttpResponse> response;
        private final IOException socketException;
        private final GeneralSecurityException sslexception;
        private final Optional<org.apache.hc.core5.http.ContentType> ct;
        private final Optional<HttpEntity> resultBody;

        private HttpResponse(HttpHost host, CloseableHttpResponse response, IOException socketException, GeneralSecurityException sslexception) {
            this.host = host;
            this.response = Optional.ofNullable(response);
            this.socketException = socketException;
            this.sslexception = sslexception;
            resultBody = this.response.map(CloseableHttpResponse::getEntity);
            ct = resultBody.map(HttpEntity::getContentType).map(org.apache.hc.core5.http.ContentType::parse);
        }
        public String getMimeType() {
            return ct.map(org.apache.hc.core5.http.ContentType::getMimeType).orElse(ContentType.APPLICATION_OCTET_STREAM.toString());
        }
        public String getHost() {
            return host.toURI();
        }
        @Override
        public void close() throws IOException {
            if (response.isPresent()) {
                response.get().close();
            }
        }
        public Reader getContentReader() throws IOException {
            try {
                return resultBody.map(e -> {
                    try {
                        return (Reader) new InputStreamReader(e.getContent(), ct.map(org.apache.hc.core5.http.ContentType::getCharset).orElse(Charset.defaultCharset()));
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }).orElse(new StringReader(""));
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
        public int getStatus() {
            return response.map(CloseableHttpResponse::getCode).orElse(-1);
        }
        public String getStatusMessage() {
            return response.map(CloseableHttpResponse::getReasonPhrase).orElse("");
        }
        public boolean isConnexionFailed() {
            return socketException != null || sslexception != null;
        }
        public IOException getSocketException() {
            return socketException;
        }
        public GeneralSecurityException getSslexception() {
            return sslexception;
        }
    }

    @MXBean
    public interface HttpClientStatsMBean {
        public int getAvailable();
        public int getLeased();
        public int getMax();
        public int getPending();
    }

    public class Implementation extends StandardMBean implements HttpClientStatsMBean {
        private final ConnPoolControl<HttpRoute> pool;
        public Implementation(ConnPoolControl<HttpRoute> pool)
                        throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(HttpClientStatsMBean.class);
            this.pool = pool;
        }
        @Override
        public int getAvailable() {
            return pool.getTotalStats().getAvailable();
        }
        @Override
        public int getLeased() {
            return pool.getTotalStats().getLeased();
        }
        @Override
        public int getMax() {
            return pool.getTotalStats().getMax();
        }
        @Override
        public int getPending() {
            return pool.getTotalStats().getPending();
        }
    }

    private final int timeout;
    private final CredentialsProvider credsProvider;

    private CloseableHttpClient client = null;
    protected final URL[] endPoints;
    private final Map<URL, HttpHost> hosts;

    public AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        timeout = builder.timeout;
        endPoints = Helpers.stringsToUrl(builder.destinations, builder.port, builder.protocol, logger);
        // Two names for login/user
        String user = builder.user != null ? builder.user : builder.login;
        if (user != null && builder.password != null) {
            BasicCredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials creds = new UsernamePasswordCredentials(user, builder.password.toCharArray());
            AuthScope scope = new AuthScope(null, -1);
            provider.setCredentials(scope, creds);
            credsProvider = provider;
        } else {
            credsProvider = null;
        }
        hosts = new ConcurrentHashMap<>(endPoints.length);
        for (URL u: endPoints) {
            HttpHost host = new HttpHost(u.getProtocol(),
                         u.getHost(),
                         u.getPort());
            hosts.put(u, host);
        }
    }

    private ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("loghub:type=Senders,servicename=" + getSenderName() + ",name=connectionsPool");
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            if (endPoints.length == 0) {
                logger.error("New usable endpoints defined");
                return false;
            }

            // Build HTTP the connection manager
            PoolingHttpClientConnectionManagerBuilder cmBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                    .setMaxConnTotal(getThreads())
                    .setMaxConnPerRoute(getThreads())
                    .setDefaultSocketConfig(SocketConfig.custom()
                                                        .setTcpNoDelay(true)
                                                        .setSoKeepAlive(true)
                                                        .setSoTimeout(timeout, TimeUnit.SECONDS)
                                                        .build())
                    .setValidateAfterInactivity(TimeValue.ofSeconds(1))
                    .setConnPoolPolicy(PoolReusePolicy.FIFO);

            if (properties.ssl != null) {
                cmBuilder.setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                         .setSslContext(properties.ssl)
                         .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
                         .build());
            }
            PoolingHttpClientConnectionManager cm = cmBuilder.build();

            try {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.registerMBean(new Implementation(cm), getObjectName());
            } catch (NotCompliantMBeanException | MalformedObjectNameException
                            | InstanceAlreadyExistsException | MBeanRegistrationException e) {
                logger.error("jmx configuration failed: " + Helpers.resolveThrowableException(e), e);
                return false;
            }

            // Build the client
            HttpClientBuilder clientBuilder = HttpClientBuilder.create();
            VersionInfo vi = VersionInfo.loadVersionInfo("org.apache.hc.client5", properties.classloader);
            clientBuilder.setUserAgent(String.format("LogHub-HttpClient/%s (Java/%s)", vi.getRelease(), System.getProperty("java.version")));
            clientBuilder.setConnectionManager(cm);
            clientBuilder.setDefaultRequestConfig(RequestConfig.custom()
                                                  .setConnectionRequestTimeout(timeout, TimeUnit.SECONDS)
                                                  .setConnectTimeout(timeout, TimeUnit.SECONDS)
                                                  .build());
            clientBuilder.disableCookieManagement();
            if (credsProvider != null) {
                clientBuilder.setDefaultCredentialsProvider(credsProvider);
            }

            client = clientBuilder.build();

            return true;
        } else {
            return false;
        }
    }

    protected HttpResponse doRequest(HttpRequest therequest) {
        HttpClientContext context = HttpClientContext.create();

        HttpHost host = hosts.computeIfAbsent(therequest.url, u -> {
           return new HttpHost(u.getProtocol(),
                               u.getHost(),
                               u.getPort());
        });

        Method method = Method.valueOf(therequest.verb.toUpperCase(Locale.ENGLISH));
        ClassicHttpRequest request = new BasicClassicHttpRequest(method, host, therequest.url.getFile());
        if (therequest.content != null) {
            request.setEntity(therequest.content);
        }
        therequest.headers.forEach(request::addHeader);
        try {
            // Don't close CloseableHttpResponse, it's handle by HttpResponse
            CloseableHttpResponse response = client.execute(host, request, context);
            return new HttpResponse(host, response, null, null);
        } catch (HttpHostConnectException e) {
            String message = "";
            try {
                if (e.getCause() != null) {
                    throw e.getCause();
                } else {
                    message = String.format("Comunication with %s failed: %s", host, Helpers.resolveThrowableException(e));
                }
            } catch (ConnectException ex) {
                message = String.format("Comunication to %s refused", host);
            } catch (SocketTimeoutException ex) {
                message = String.format("Slow response from %s", host);
            } catch (Throwable ex) {
                // Don't worry, it was wrapped in HttpHostConnectException, so we're never catching a fatal exception here
                message = String.format("Connection to %s failed: %s", host, Helpers.resolveThrowableException(ex));
            }
            logger.error(message);
            logger.catching(Level.DEBUG, e.getCause());
            return new HttpResponse(host, null, e, null);
        } catch (IOException e) {
            Throwable rootCause = e;
            while (rootCause.getCause() != null){
                rootCause = rootCause.getCause();
            };
            // A TLS exception, will not help to retry
            if (rootCause instanceof GeneralSecurityException) {
                logger.error("Secure comunication with {} failed: {}", host, Helpers.resolveThrowableException(rootCause));
                logger.catching(Level.DEBUG, rootCause);
                return new HttpResponse(host, null, null, (GeneralSecurityException) rootCause);
            } else {
                logger.error("Comunication with {} failed: {}", host, Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
                return new HttpResponse(host, null, e, null);
            }
        }
    }

    @Override
    public void customStopSending() {
        client.close(CloseMode.GRACEFUL);
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(getObjectName());
        } catch (InstanceNotFoundException e) {
            logger.debug("Failed to unregister mbeam: "
                         + Helpers.resolveThrowableException(e), e);
        } catch (MalformedObjectNameException
                        | MBeanRegistrationException e) {
            logger.error("Failed to unregister mbeam: "
                         + Helpers.resolveThrowableException(e), e);
            logger.catching(Level.DEBUG, e);
        }
    };

}
