package loghub;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.configuration.Properties;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.httpclient.JavaHttpClientService;
import loghub.metrics.JmxService;
import loghub.security.ssl.SslContextBuilder;
import lombok.Getter;

public abstract class AbstractDashboard {

    // Jackson use JUL for logging
    static  {
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Dashboard", "loghub.netty");
    }

    private static final JsonFactory factory = new JsonFactory();
    @Getter
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    @Getter
    private static final Properties props = new Properties(Collections.emptyMap());
    private Dashboard dashboard = null;
    @Getter
    private int port;
    @Getter
    private String scheme;
    @Getter
    SSLContext sslContext;
    @Getter
    HttpClient client;

    @AfterAll
    public static void stopJmx() {
        JmxService.stop();
    }

    @AfterAll
    public static void cleanState() {
        System.clearProperty("java.util.logging.manager");
    }

    @BeforeEach
    public void startDashBoard() throws IllegalArgumentException, InterruptedException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.3");
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.putAll(props);
        sslContext = SslContextBuilder.getBuilder(properties).build();

        dashboard = Dashboard.getBuilder()
                             .setWithSSL(withSsl())
                             .setSslContext(sslContext)
                             .setPort(0)
                             .setListen("localhost")
                             .build();
        dashboard.start();
        port = ((InetSocketAddress) dashboard.getTransport().getChannels().findFirst().orElseThrow().localAddress()).getPort();
        scheme = getDashboardScheme();
        client = HttpClient.newBuilder()
                           .sslContext(sslContext)
                           .followRedirects(HttpClient.Redirect.NEVER)
                           .build();
    }

    protected abstract boolean withSsl();

    protected abstract String getDashboardScheme();

    @AfterEach
    public void stopDashboard() {
        Optional.ofNullable(dashboard).ifPresent(Dashboard::stop);
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    public void getIndex(HttpClient.Version version) throws IllegalArgumentException, IOException, InterruptedException {
        URI theurl = URI.create(String.format("%s://localhost:%d/static/index.html", scheme, port));
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                                                    .uri(theurl)
                                                    .version(version)
                                                    .build();
        java.net.http.HttpResponse<String> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertTrue(response.body().startsWith("<!DOCTYPE html>"));
    }

    @Test
    @Timeout(5)
    public void getIndexWithClient() throws IllegalArgumentException, IOException {
        JavaHttpClientService.Builder builder = JavaHttpClientService.getBuilder();
        builder.setSslContext(sslContext);
        AbstractHttpClientService httpClient = builder.build();
        HttpRequest<Object> req = httpClient.getRequest();
        req.setUri(URI.create(String.format("%s://localhost:%d/static/index.html", scheme, port)));
        req.setConsumeText(r -> {
            try (BufferedReader reader = new BufferedReader(r)) {
                StringBuilder buf = new StringBuilder();
                reader.lines().forEach(buf::append);
                Assertions.assertTrue(buf.toString().startsWith("<!DOCTYPE html>"));
                return null;
            }
        });
        try (HttpResponse<Object> rep = httpClient.doRequest(req)) {
            Assertions.assertEquals(200, rep.getStatus());
            Assertions.assertEquals(ContentType.TEXT_HTML, rep.getMimeType());
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    public void getFailure1(HttpClient.Version version) throws IOException, InterruptedException {
        URI theurl = URI.create(String.format("%s://localhost:%d/metric/1", scheme, port));
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                                                    .uri(theurl)
                                                    .version(version)
                                                    .build();
        java.net.http.HttpResponse<String> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("Unsupported metric name: 1", response.body().trim());
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    public void getFailure2(HttpClient.Version version) throws IOException, InterruptedException {
        URI theurl = URI.create(String.format("%s://localhost:%d/metric/stranges", scheme, port));
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                                                    .uri(theurl)
                                                    .version(version)
                                                    .build();
        java.net.http.HttpResponse<String> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("Unsupported metric name: stranges", response.body().trim());
    }

    @Test
    @Timeout(5)
    public void jolokia() throws IOException {
        JavaHttpClientService.Builder builder = JavaHttpClientService.getBuilder();
        builder.setTimeout(10000000);
        builder.setSslContext(sslContext);
        AbstractHttpClientService httpClient = builder.build();

        try (HttpResponse<Map<String, ?>> rep = runRequest(httpClient, "GET", "/version", null)) {
            Assertions.assertEquals(404, rep.getStatus());
        }
    }

    private HttpResponse<Map<String, ?>> runRequest(AbstractHttpClientService client, String verb, String path, String bodypost) {
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
