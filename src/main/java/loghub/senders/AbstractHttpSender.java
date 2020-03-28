package loghub.senders;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.pool.ConnPoolControl;
import org.apache.http.util.VersionInfo;
import org.apache.logging.log4j.Level;

import loghub.Helpers;
import loghub.configuration.Properties;
import lombok.Setter;

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

    protected class HttpRequest {
        private String verb = "GET";
        private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
        private URL url = null;
        private final Map<String, String> headers = new HashMap<>();
        private HttpEntity content = null;
        public String getVerb() {
            return verb;
        }
        public void setVerb(String verb) {
            this.verb = verb.toUpperCase().intern();
        }
        public String getHttpVersion() {
            return httpVersion.toString();
        }
        public void setHttpVersion(int major, int minor) {
            this.httpVersion = (HttpVersion) httpVersion.forVersion(major, minor);
        }
        public URL getUrl() {
            return url;
        }
        public void setUrl(URL url) {
            this.url = url;
        }
        public void addHeader(String header, String value) {
            headers.put(header, value);
        }
        public void clearHeaders() {
            headers.clear();
        }
        public void setContent(byte[] content) {
            this.content = new ByteArrayEntity(content);
        }
        public void setTypeAndContent(String mimeType, Charset charset, byte[] content) throws IOException {
            EntityBuilder builder = EntityBuilder.create()
                            .setBinary(content)
                            .setContentType(org.apache.http.entity.ContentType.create(mimeType, charset));
            this.content = builder.build();
        }
    }

    protected enum ContentType {

        APPLICATION_OCTET_STREAM (org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM ),
        APPLICATION_JSON(org.apache.http.entity.ContentType.APPLICATION_JSON),
        TEXT_XML(org.apache.http.entity.ContentType.TEXT_XML);

        private final org.apache.http.entity.ContentType realType;

        ContentType(org.apache.http.entity.ContentType realType) {
            this.realType = realType;
        }

        @Override
        public String toString() {
            return realType.toString();
        }

    };

    protected class HttpResponse implements Closeable {
        private final HttpHost host;
        private final CloseableHttpResponse response;
        private final IOException socketException;
        private final GeneralSecurityException sslexception;

        private HttpResponse(HttpHost host, CloseableHttpResponse response, IOException socketException, GeneralSecurityException sslexception) {
            super();
            this.host = host;
            this.response = response;
            this.socketException = socketException;
            this.sslexception = sslexception;
        }
        public String getMimeType() {
            HttpEntity resultBody = response.getEntity();
            org.apache.http.entity.ContentType ct = org.apache.http.entity.ContentType.get(resultBody);
            if (ct !=  null) {
                return ct.getMimeType();
            } else {
                return "";
            }
        }
        public String getHost() {
            return host.toURI();
        }
        public void close() {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        public Reader getContentReader() throws IOException {
            HttpEntity resultBody = response.getEntity();
            org.apache.http.entity.ContentType ct = org.apache.http.entity.ContentType.get(resultBody);
            Charset charset = ct.getCharset();
            if (charset == null) {
                charset = Charset.defaultCharset();
            }
            return new InputStreamReader(resultBody.getContent(), charset);
        }
        public int getStatus() {
            return response.getStatusLine().getStatusCode();
        }
        public String getStatusMessage() {
            return response.getStatusLine().getReasonPhrase();
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

    // Beans
    private final int timeout;
    private CredentialsProvider credsProvider = null;

    private CloseableHttpClient client = null;
    protected final URL[] endPoints;

    public AbstractHttpSender(Builder<? extends AbstractHttpSender> builder) {
        super(builder);
        timeout = builder.timeout;
        endPoints = Helpers.stringsToUrl(builder.destinations, builder.port, builder.protocol, logger);
        // Two names for login/user
        String user = builder.user != null ? builder.user : builder.login;
        if (user != null && builder.password != null) {
            credsProvider = new BasicCredentialsProvider();
            for(URL i: endPoints) {
                credsProvider.setCredentials(
                                             new AuthScope(i.getHost(), i.getPort()), 
                                             new UsernamePasswordCredentials(user, builder.password));
            }
        }

    }

    @Override
    public boolean configure(Properties properties) {

        if(endPoints.length == 0) {
            return false;
        }

        // The HTTP connection management
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.setUserAgent(VersionInfo.getUserAgent("LogHub-HttpClient",
                                                            "org.apache.http.client", HttpClientBuilder.class));

        // Set the Configuration manager
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", PlainConnectionSocketFactory.getSocketFactory())
                        .register("https", new SSLConnectionSocketFactory(properties.ssl))
                        .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
        cm.setMaxTotal(getThreads() + 1);
        cm.setDefaultMaxPerRoute(getThreads() + 1);
        cm.setValidateAfterInactivity(timeout * 1000);
        clientBuilder.setConnectionManager(cm);

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new Implementation(cm), new ObjectName("loghub:type=sender,servicename=" + getName() + ",name=connectionsPool"));
        } catch (NotCompliantMBeanException | MalformedObjectNameException
                        | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            throw new RuntimeException("jmx configuration failed: " + Helpers.resolveThrowableException(e), e);
        }

        if (properties.ssl != null) {
            clientBuilder.setSSLContext(properties.ssl);
        }

        clientBuilder.setDefaultRequestConfig(RequestConfig.custom()
                                              .setConnectionRequestTimeout(timeout * 1000)
                                              .setConnectTimeout(timeout * 1000)
                                              .setSocketTimeout(timeout * 1000)
                                              .build());
        clientBuilder.setDefaultSocketConfig(SocketConfig.custom()
                                             .setTcpNoDelay(true)
                                             .setSoKeepAlive(true)
                                             .setSoTimeout(timeout * 1000)
                                             .build());
        clientBuilder.setDefaultConnectionConfig(ConnectionConfig.custom()
                                                 .build());
        clientBuilder.disableCookieManagement();

        clientBuilder.setRetryHandler((i,j, k) -> false);

        client = clientBuilder.build();

        return true;
    }

    protected HttpResponse doRequest(HttpRequest therequest) {

        HttpClientContext context = HttpClientContext.create();
        if (credsProvider != null) {
            context.setCredentialsProvider(credsProvider);
        }

        RequestLine requestLine = new BasicRequestLine(therequest.verb, therequest.url.getPath(), therequest.httpVersion);
        BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(requestLine);
        if (therequest.content != null) {
            request.setEntity(therequest.content);
        }
        therequest.headers.forEach((i,j) -> request.addHeader(i, j));
        HttpHost host = new HttpHost(therequest.url.getHost(),
                            therequest.url.getPort(),
                            therequest.url.getProtocol());
        try {
            CloseableHttpResponse response = client.execute(host, request, context);
            return new HttpResponse(host, response, null, null);
        } catch (ConnectionPoolTimeoutException e) {
            logger.error("All connections slots to {} used.", host);
            return new HttpResponse(host, null, e, null);
        } catch (HttpHostConnectException e) {
            String message = "";
            try {
                throw e.getCause();
            } catch (ConnectException e1) {
                message = String.format("Connection to %s refused", host);
            } catch (SocketTimeoutException e1) {
                message = String.format("Slow response from %s", host);
            } catch (Throwable e1) {
                // Don't worry, it was wrapped in HttpHostConnectException, so we're never catching a fatal exception here
                message = String.format("Connection to %s failed: %s", host, Helpers.resolveThrowableException(e1));
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
    };

}
