package loghub.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.hc.client5.http.HttpHostConnectException;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.IOCallback;
import org.apache.hc.core5.pool.ConnPoolControl;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.VersionInfo;
import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.Helpers;
import lombok.Getter;
import lombok.experimental.Accessors;

@BuilderClass(ApacheHttpClientService.Builder.class)
public class ApacheHttpClientService extends AbstractHttpClientService {

    @MXBean
    public interface HttpClientStatsMBean {
        int getAvailable();

        int getLeased();

        int getMax();

        int getPending();
    }

    public static class Builder extends AbstractHttpClientService.Builder<ApacheHttpClientService> {
        @Override
        public ApacheHttpClientService build() {
            return new ApacheHttpClientService(this);
        }
    }
    public static ApacheHttpClientService.Builder getBuilder() {
        return new ApacheHttpClientService.Builder();
    }

    @Accessors(fluent = false, chain = true)
    private static class HcHttpRequest<T> extends HttpRequest<T> {
        private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
        private final List<BasicHeader> headers = new ArrayList<>();
        private HttpEntity content;

        public String getHttpVersion() {
            return httpVersion.toString();
        }
        public HcHttpRequest<T> setHttpVersion(int major, int minor) {
            this.httpVersion = HttpVersion.get(major, minor);
            return this;
        }
        public HcHttpRequest<T> addHeader(String header, String value) {
            headers.add(new BasicHeader(header, value));
            return this;
        }
        public HcHttpRequest<T> clearHeaders() {
            headers.clear();
            return this;
        }
        public HcHttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content) {
            this.content = HttpEntities.createGzipped(content, mapContentType(mimeType));
            return this;
        }
        public HcHttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source) {
            IOCallback<OutputStream> cp = source::writeTo;
            content = HttpEntities.createGzipped(cp, mapContentType(mimeType));
            return this;
        }
        private org.apache.hc.core5.http.ContentType mapContentType(ContentType ct) {
            switch (ct) {
            case APPLICATION_JSON:
                return org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
            case APPLICATION_OCTET_STREAM:
                return org.apache.hc.core5.http.ContentType.APPLICATION_OCTET_STREAM;
            case APPLICATION_XML:
                return org.apache.hc.core5.http.ContentType.APPLICATION_XML;
            case TEXT_PLAIN:
                return org.apache.hc.core5.http.ContentType.TEXT_PLAIN;
            case TEXT_HTML:
                return org.apache.hc.core5.http.ContentType.TEXT_HTML;
            default:
                throw new IllegalArgumentException("Unknown content type");
            }
        }
    }

    @lombok.Builder
    @Accessors(fluent = false, chain = true)
    private static class HcHttpResponse<T> extends loghub.httpclient.HttpResponse<T> {
        private final HttpHost host;
        private final ClassicHttpResponse response;
        @Getter
        private final IOException socketException;
        @Getter
        private final GeneralSecurityException sslException;
        @Getter
        private final ContentType mimeType;
        @Getter
        private final T parsedResponse;

        @Override
        public String getHost() {
            return host.getHostName();
        }

        @Override
        public void close() throws IOException {
            if (response != null) {
                response.close();
            }
        }

        @Override
        public int getStatus() {
            if (response != null) {
                return response.getCode();
            } else {
                throw new IllegalStateException(socketException);
            }
        }

        @Override
        public String getStatusMessage() {
            if (response != null) {
                return response.getReasonPhrase();
            } else {
                throw new IllegalStateException(socketException);
            }
        }

        @Override
        public boolean isConnexionFailed() {
            return socketException != null || sslException != null;
        }
    }

    private static class Implementation extends StandardMBean implements HttpClientStatsMBean {
        private final ConnPoolControl<HttpRoute> pool;
        public Implementation(ConnPoolControl<HttpRoute> pool) throws NotCompliantMBeanException {
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

    private final CloseableHttpClient client;
    private final Map<URI, HttpHost> hosts;

    private ApacheHttpClientService(Builder builder) {
        super(builder);
        // Two names for login/user
        String user = builder.user;
        CredentialsProvider credsProvider;
        if (user != null && builder.password != null) {
            BasicCredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials creds = new UsernamePasswordCredentials(user, builder.password.toCharArray());
            AuthScope scope = new AuthScope(null, -1);
            provider.setCredentials(scope, creds);
            credsProvider = provider;
        } else {
            credsProvider = null;
        }
        hosts = new ConcurrentHashMap<>();

        // Build HTTP the connection manager
        PoolingHttpClientConnectionManagerBuilder cmBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                                                                      .setMaxConnTotal(builder.workers)
                                                                      .setMaxConnPerRoute(builder.workers)
                                                                      .setDefaultSocketConfig(SocketConfig.custom()
                                                                                                      .setTcpNoDelay(true)
                                                                                                      .setSoKeepAlive(true)
                                                                                                      .setSoTimeout(timeout, TimeUnit.SECONDS)
                                                                                                      .build())
                                                                      .setDefaultConnectionConfig(ConnectionConfig.custom()
                                                                                                          .setValidateAfterInactivity(TimeValue.ofSeconds(1))
                                                                                                          .setSocketTimeout(timeout, TimeUnit.SECONDS)
                                                                                                          .setConnectTimeout(timeout, TimeUnit.SECONDS)
                                                                                                          .build())
                                                                      .setConnPoolPolicy(PoolReusePolicy.FIFO);
        if (builder.sslContext != null) {
            SSLContext sslContext =  resolveSslContext(builder);
            SSLParameters sslParams = resolveSslParams(builder, sslContext);
            cmBuilder.setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                                                  .setSslContext(builder.sslContext)
                                                  .setCiphers(sslParams.getCipherSuites())
                                                  .build());
        }
        PoolingHttpClientConnectionManager cm = cmBuilder.build();
        try {
            if (builder.jmxParent != null) {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                Hashtable<String, String> parentProps = builder.jmxParent.getKeyPropertyList();
                parentProps.put("name", "connectionsPool");
                ObjectName subName = new ObjectName(builder.jmxParent.getDomain(), parentProps);
                mbs.registerMBean(new Implementation(cm), subName);
            }
        } catch (NotCompliantMBeanException | MalformedObjectNameException
                 | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            throw new IllegalStateException("jmx configuration failed: " + Helpers.resolveThrowableException(e), e);
        }

        // Build the client
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        VersionInfo vi = VersionInfo.loadVersionInfo("org.apache.hc.client5", getClass().getClassLoader());
        clientBuilder.setUserAgent(String.format("LogHub-HttpClient/%s (Java/%s)", vi.getRelease(), System.getProperty("java.version")));
        clientBuilder.setConnectionManager(cm);
        clientBuilder.setDefaultRequestConfig(RequestConfig.custom()
                                                      .setConnectionRequestTimeout(timeout, TimeUnit.SECONDS)
                                                      .build());
        clientBuilder.disableCookieManagement();
        if (credsProvider != null) {
            clientBuilder.setDefaultCredentialsProvider(credsProvider);
        }

        client = clientBuilder.build();
    }

    @Override
    public <T> HttpRequest<T> getRequest() {
        return new HcHttpRequest<>();
    }

    @Override
    public <T> HttpResponse<T> doRequest(HttpRequest<T> therequest) {
        HcHttpResponse.HcHttpResponseBuilder<T> builder = HcHttpResponse.builder();
        @SuppressWarnings("unchecked")
        HcHttpRequest<HcHttpResponse<T>> hcrequest = (HcHttpRequest<HcHttpResponse<T>>) therequest;
        HttpClientContext context = HttpClientContext.create();

        HttpHost host = hosts.computeIfAbsent(therequest.uri,
                u -> new HttpHost(u.getScheme(), u.getHost(), u.getPort())
        );
        builder.host(host);
        Method method = Method.valueOf(therequest.verb.toUpperCase(Locale.ENGLISH));
        ClassicHttpRequest request = new BasicClassicHttpRequest(method, therequest.uri);
        if (hcrequest.content != null) {
            request.setEntity(hcrequest.content);
        }
        hcrequest.headers.forEach(request::addHeader);
        try {
            // Don't close CloseableHttpResponse, it's handle by HttpResponse
            return client.execute(host, request, context, t -> resolve(builder, therequest, t).build());
        } catch (HttpHostConnectException e) {
            String message;
            try {
                if (e.getCause() != null) {
                    throw e.getCause();
                } else {
                    message = String.format("Communication with %s failed: %s", host, Helpers.resolveThrowableException(e));
                }
            } catch (ConnectException ex) {
                message = String.format("Communication to %s refused", host);
            } catch (SocketTimeoutException ex) {
                message = String.format("Slow response from %s", host);
            } catch (Throwable ex) {
                // Don't worry, it was wrapped in HttpHostConnectException, so we're never catching a fatal exception here
                message = String.format("Connection to %s failed: %s", host, Helpers.resolveThrowableException(ex));
            }
            logger.error(message);
            logger.catching(Level.DEBUG, e.getCause());
            return builder.socketException(e).build();
        } catch (IOException e) {
            Throwable rootCause = e;
            while (rootCause.getCause() != null){
                rootCause = rootCause.getCause();
            }
            // A TLS exception, will not help to retry
            if (rootCause instanceof GeneralSecurityException) {
                logger.error("Secure communication with {} failed: {}", host, Helpers.resolveThrowableException(rootCause));
                logger.catching(Level.DEBUG, rootCause);
                return builder.sslException((GeneralSecurityException) rootCause).build();
            } else {
                logger.error("Communication with {} failed: {}", host, Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
                return builder.socketException(e).build();
            }
        }
    }

    private <T> HcHttpResponse.HcHttpResponseBuilder<T> resolve(HcHttpResponse.HcHttpResponseBuilder<T> builder, HttpRequest<T> request, ClassicHttpResponse response)
            throws IOException {
        builder.response(response);
        HttpEntity content = response.getEntity();
        if (content != null) {
            org.apache.hc.core5.http.ContentType ct = org.apache.hc.core5.http.ContentType.parse(content.getContentType());
            ContentType mimeType = resolveMimeType(content);
            builder.mimeType(mimeType);
            try (InputStream contentStream = content.getContent()) {
                if (mimeType.isTextBody()) {
                    Charset cs = ct.getCharset(StandardCharsets.UTF_8);
                    builder.parsedResponse(request.consumeText.read(new InputStreamReader(contentStream, cs)));
                } else {
                    builder.parsedResponse(request.consumeBytes.read(contentStream));
                }
            } catch (IOException e) {
                builder.socketException(e);
            }
        }
        return builder;
    }

    private ContentType resolveMimeType(HttpEntity content) {
        org.apache.hc.core5.http.ContentType ct = org.apache.hc.core5.http.ContentType.parse(content.getContentType());
        switch (ct.getMimeType()) {
        case AbstractHttpClientService.APPLICATION_JSON:
            return ContentType.APPLICATION_JSON;
        case AbstractHttpClientService.APPLICATION_XML:
            return ContentType.APPLICATION_XML;
        case AbstractHttpClientService.APPLICATION_OCTET_STREAM:
            return ContentType.APPLICATION_OCTET_STREAM;
        case AbstractHttpClientService.TEXT_HTML:
            return ContentType.TEXT_HTML;
        case AbstractHttpClientService.TEXT_PLAIN:
            return ContentType.TEXT_PLAIN;
        default:
            throw new IllegalStateException("Unhandled content type: " + content.getContentType());
        }
    }

    @Override
    public void customStopSending() {
        client.close(CloseMode.GRACEFUL);
    }

}
