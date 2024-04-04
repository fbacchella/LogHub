package loghub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.configuration.Properties;
import loghub.httpclient.ApacheHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.metrics.JmxService;
import loghub.security.ssl.SslContextBuilder;
import lombok.Getter;

public abstract class AbstractDashboard {

    // Jackson use JUL for logging
    static  {
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    private static final SSLContext previousSslContext;
    static {
        try {
            previousSslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static final JsonFactory factory = new JsonFactory();
    @Getter
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    @Getter
    private final static Properties props = new Properties(Collections.emptyMap());
    private Dashboard dashboard = null;
    @Getter
    private int port;
    @Getter
    private String scheme;
    @Getter
    SSLContext sslContext;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Dashboard", "loghub.netty");
        JmxService.start(props.jmxServiceConfiguration);
    }

    @AfterClass
    static public void stopJmx() {
        JmxService.stop();
    }

    @AfterClass
    static public void cleanState() {
        System.clearProperty("java.util.logging.manager");
        SSLContext.setDefault(previousSslContext);
    }

    @Before
    public void startDashBoard() throws IllegalArgumentException, InterruptedException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.3");
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.putAll(props);
        sslContext = SslContextBuilder.getBuilder(properties).build();
        SSLContext.setDefault(sslContext);

        dashboard = Dashboard.getBuilder()
                             .setWithSSL(withSsl())
                             .setSslContext(sslContext)
                             .setPort(0)
                             .setListen("localhost")
                             .setWithJolokia(false)
                             .build();
        dashboard.start();
        port = ((InetSocketAddress)dashboard.getTransport().getChannels().findFirst().get().localAddress()).getPort();
        scheme = getDashboardScheme();
    }

    protected abstract boolean withSsl();

    protected abstract String getDashboardScheme();

    @After
    public void stopDashboard() {
        Optional.ofNullable(dashboard).ifPresent(Dashboard::stop);
    }

    @Test
    public void getIndex() throws IllegalArgumentException, IOException {
        URL theurl = URI.create(String.format("%s://localhost:%d/static/index.html", scheme, port)).toURL();
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(200, cnx.getResponseCode());
        String page = readContent(cnx);
        Assert.assertTrue(page.startsWith("<!DOCTYPE html>"));
        cnx.disconnect();
    }

    @Test
    public void getIndexWithClient() throws IllegalArgumentException, IOException {
        ApacheHttpClientService.Builder builder = ApacheHttpClientService.getBuilder();
        builder.setSslContext(sslContext);
        ApacheHttpClientService client = builder.build();
        HttpRequest<Object> req = client.getRequest();
        req.setUri(URI.create(String.format("%s://localhost:%d/static/index.html", scheme, port)));
        req.setConsumeText(r -> {
            try (BufferedReader reader = new BufferedReader(r)) {
                StringBuilder buf = new StringBuilder();
                reader.lines().forEach(buf::append);
                Assert.assertTrue(buf.toString().startsWith("<!DOCTYPE html>"));
                return null;
            }
        });
        try (HttpResponse<Object> rep = client.doRequest(req)) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.TEXT_HTML, rep.getMimeType());
        }
    }
    @Test
    public void getFailure1() throws IOException {
        URL theurl = URI.create(String.format("%s://localhost:%d/metric/1", scheme, port)).toURL();
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(400, cnx.getResponseCode());
        Assert.assertEquals("Unsupported metric name: 1", readErrorContent(cnx));
        cnx.disconnect();
    }

    @Test
    public void getFailure2() throws IOException {
        URL theurl = URI.create(String.format("%s://localhost:%d/metric/stranges", scheme, port)).toURL();
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(400, cnx.getResponseCode());
        Assert.assertEquals("Unsupported metric name: stranges", readErrorContent(cnx));
        cnx.disconnect();
    }

    protected String readContent(HttpURLConnection cnx) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(cnx.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder buf = new StringBuilder();
            reader.lines().forEach(buf::append);
            return buf.toString();
        }
    }

    private String readErrorContent(HttpURLConnection cnx) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(cnx.getErrorStream(), StandardCharsets.UTF_8))) {
            StringBuilder buf = new StringBuilder();
            reader.lines().forEach(buf::append);
            return buf.toString();
        }
    }

    @Test
    public void jolokia() throws IOException {
        ApacheHttpClientService.Builder builder = ApacheHttpClientService.getBuilder();
        builder.setTimeout(10000000);
        builder.setSslContext(sslContext);
        ApacheHttpClientService client = builder.build();

        try (HttpResponse<Map<String, ?>> rep = runRequest(client, "GET", "/version", null)) {
            Assert.assertEquals(404, rep.getStatus());
        }
    }

    private HttpResponse<Map<String, ?>> runRequest(ApacheHttpClientService client, String verb, String path, String bodypost) {
        HttpRequest<Map<String, ?>> request = client.getRequest();
        request.setUri(URI.create(String.format("%s://localhost:%d/jolokia%s", scheme, port, path)));
        request.setVerb(verb);
        request.setConsumeText(r -> json.get().reader().readValue(r, Map.class));
        if (bodypost != null && ! bodypost.isEmpty()) {
            request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> os.write(bodypost.getBytes(StandardCharsets.UTF_8)));
        }
        return client.doRequest(request);
    }

}
