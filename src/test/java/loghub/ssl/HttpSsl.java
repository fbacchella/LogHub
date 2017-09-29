package loghub.ssl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Supplier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.VersionInfo;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;

public class HttpSsl {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.ssl");
        Configurator.setLevel("org", Level.WARN);
    }

    HttpRequestHandler requestHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if(request instanceof BasicHttpEntityEnclosingRequest) {
                BasicHttpEntityEnclosingRequest jsonrequest = (BasicHttpEntityEnclosingRequest) request;
                EntityUtils.consume(jsonrequest.getEntity());
            }
            response.setStatusCode(200);
            response.setHeader("Content-Type", "application/json; charset=UTF-8");
            response.setEntity(new StringEntity("{}"));
        }
    };

    private static final URL theurl;
    static {
        try {
            theurl = new URL("https://localhost:15716/");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    Supplier<SSLContext> getContext = () -> {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.2");
        properties.put("trusts", new String[] {getClass().getResource("/localhost.p12").getFile()});
        SSLContext newCtxt = ContextLoader.build(properties);
        Assert.assertEquals("TLSv1.2", newCtxt.getProtocol());
        return newCtxt;
    };

    @Rule
    public ExternalResource resource = new HttpTestServer(getContext.get(), 15716, new HttpTestServer.HandlerInfo("/", requestHandler));

    private static final HostnameVerifier localhostVerifier = new HostnameVerifier() {
        @Override
        public boolean verify(String arg0, SSLSession arg1) {
            try {
                return "CN=localhost".equals(arg1.getPeerPrincipal().getName());
            } catch (SSLPeerUnverifiedException e) {
                throw new RuntimeException(e);
            }
        }
    };

    @Test
    public void TestSimple() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        SSLContext ctx = getContext.get();
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(ctx.getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
    }

    @Test
    public void TestHcSimple() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        SSLContext sslcontext = getContext.get();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext, localhostVerifier);
        CloseableHttpClient httpclient = HttpClients.custom()
                .setSSLSocketFactory(sslsf)
                .build();
        try {
            HttpGet httpget = new HttpGet(theurl.toString());
            HttpClientContext context = HttpClientContext.create();
            CloseableHttpResponse response = httpclient.execute(httpget, context);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            try {
                HttpEntity entity = response.getEntity();
                EntityUtils.consume(entity);
            } finally {
                response.close();
            }
        } finally {
            httpclient.close();
        }
    }

    @Test
    public void TestHcComplex() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        CloseableHttpClient client = null;;
        try {
            int publisherThreads = 1;
            int timeout = 2;
            // The HTTP connection management
            HttpClientBuilder builder = HttpClientBuilder.create();
            builder.setUserAgent(VersionInfo.getUserAgent("LogHub-HttpClient",
                    "org.apache.http.client", HttpClientBuilder.class));

            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", new SSLConnectionSocketFactory(getContext.get(), localhostVerifier))
                    .build();

            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
            cm.setDefaultMaxPerRoute(2);
            cm.setMaxTotal( 2 * publisherThreads);
            cm.setValidateAfterInactivity(timeout * 1000);
            builder.setConnectionManager(cm);

            builder.setDefaultRequestConfig(RequestConfig.custom()
                    .setConnectionRequestTimeout(timeout * 1000)
                    .setConnectTimeout(timeout * 1000)
                    .setSocketTimeout(timeout * 1000)
                    .build());
            builder.setDefaultSocketConfig(SocketConfig.custom()
                    .setTcpNoDelay(true)
                    .setSoKeepAlive(true)
                    .setSoTimeout(timeout * 1000)
                    .build());
            builder.setDefaultConnectionConfig(ConnectionConfig.custom()
                    .setCharset(Charset.forName("UTF-8"))
                    .build());
            builder.disableCookieManagement();

            client = builder.build();

            HttpClientContext context = HttpClientContext.create();
            HttpHost host;
            RequestLine requestLine = new BasicRequestLine("GET", theurl.getPath(), HttpVersion.HTTP_1_1);
            BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(requestLine);
            host = new HttpHost(theurl.getHost(),
                    theurl.getPort(),
                    "https");
            CloseableHttpResponse response = client.execute(host, request, context);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            try {
                HttpEntity entity = response.getEntity();
                EntityUtils.consume(entity);
            } finally {
                response.close();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

}
