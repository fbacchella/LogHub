package loghub.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Hashtable;
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
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.ConnPoolControl;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.VersionInfo;
import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.Helpers;

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
    private final RequestConfig config;

    private ApacheHttpClientService(Builder builder) {
        super(builder);
        CredentialsProvider credsProvider;
        if (builder.getUser() != null && builder.getPassword() != null) {
            BasicCredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials creds = new UsernamePasswordCredentials(builder.getUser(), builder.getPassword().toCharArray());
            AuthScope scope = new AuthScope(null, -1);
            provider.setCredentials(scope, creds);
            credsProvider = provider;
        } else {
            credsProvider = null;
        }
        hosts = new ConcurrentHashMap<>();

        // Build HTTP the connection manager
        PoolingHttpClientConnectionManagerBuilder cmBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                                                                      .setMaxConnTotal(builder.getWorkers())
                                                                      .setMaxConnPerRoute(builder.getWorkers())
                                                                      .setDefaultSocketConfig(SocketConfig.custom()
                                                                                                      .setTcpNoDelay(true)
                                                                                                      .setSoKeepAlive(true)
                                                                                                      .setSoTimeout(builder.getTimeout(), TimeUnit.SECONDS)
                                                                                                      .build())
                                                                      .setDefaultConnectionConfig(ConnectionConfig.custom()
                                                                                                          .setValidateAfterInactivity(TimeValue.ofSeconds(1))
                                                                                                          .setSocketTimeout(builder.getTimeout(), TimeUnit.SECONDS)
                                                                                                          .setConnectTimeout(builder.getTimeout(), TimeUnit.SECONDS)
                                                                                                          .build())
                                                                      .setConnPoolPolicy(PoolReusePolicy.FIFO);
        SSLContext sslContext =  resolveSslContext(builder);
        SSLParameters sslParams = resolveSslParams(builder, sslContext);
        cmBuilder.setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                                                                       .setSslContext(sslContext)
                                                                       .setCiphers(sslParams.getCipherSuites())
                                                                       .build());
        PoolingHttpClientConnectionManager cm = cmBuilder.build();
        try {
            if (builder.getJmxParent() != null) {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                Hashtable<String, String> parentProps = builder.getJmxParent().getKeyPropertyList();
                parentProps.put("name", "connectionsPool");
                ObjectName subName = new ObjectName(builder.getJmxParent().getDomain(), parentProps);
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
        this.config = RequestConfig.custom()
                                   .setConnectionRequestTimeout(builder.getTimeout(), TimeUnit.SECONDS)
                                   .build();
        clientBuilder.setDefaultRequestConfig(config);
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

        HttpHost host = hosts.computeIfAbsent(therequest.getUri(),
                u -> new HttpHost(u.getScheme(), u.getHost(), u.getPort())
        );
        builder.host(host);
        Method method = Method.valueOf(therequest.getVerb().toUpperCase(Locale.ENGLISH));
        HttpUriRequestBase request = new HttpUriRequestBase(method.toString(), therequest.getUri());
        if (hcrequest.getRequestTimeout() > 0) {
            request.setConfig(RequestConfig.copy(config).setResponseTimeout(hcrequest.getRequestTimeout(), TimeUnit.SECONDS).build());
        }
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
                    builder.parsedResponse(request.getConsumeText().read(new InputStreamReader(contentStream, cs)));
                } else {
                    builder.parsedResponse(request.getConsumeBytes().read(contentStream));
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
