package loghub.prometheus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.prometheus.metrics.expositionformats.PrometheusProtobufWriter;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import loghub.Dashboard;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.httpclient.JavaHttpClientService;
import loghub.metrics.JmxService;

public class TestDashboard {

    static {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Dashboard", "loghub.netty", "io.prometheus");
    }

    private static final Properties props = new Properties(Collections.emptyMap());
    private Dashboard dashboard = null;
    private final int port = Tools.tryGetPort();

    @BeforeClass
    public static void configure() throws IOException {
        JmxService.start(props.jmxServiceConfiguration);
    }

    @AfterClass
    public static void stopJmx() {
        JmxService.stop();
    }

    @Before
    public void startDashBoard() throws IllegalArgumentException, InterruptedException {
        Map<String, Object> promProps = Map.ofEntries(
                Map.entry("prometheus.withExporter", true),
                Map.entry("prometheus.withJvmMetrics", true)
        );
        dashboard = Dashboard.getBuilder()
                             .setPort(port)
                             .setListen("localhost")
                             .setDashboardServicesProperties(new HashMap<>(promProps))
                             .build();
        dashboard.start();
    }

    @After
    public void stopDashBoard() {
        Optional.ofNullable(dashboard).ifPresent(Dashboard::stop);
    }

    @Test(timeout = 5000)
    public void prometheusProtoBuf() throws IOException {
        JavaHttpClientService.Builder builder = JavaHttpClientService.getBuilder();
        AbstractHttpClientService client = builder.build();

        try (HttpResponse<BufferedReader> rep = runRequest(client, "GET", "/metrics",
                PrometheusProtobufWriter.CONTENT_TYPE)) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.APPLICATION_OCTET_STREAM, rep.getMimeType());
        }
    }

    @Test//(timeout = 5000)
    public void prometheusOpenMetrics() throws IOException {
        JavaHttpClientService.Builder builder = JavaHttpClientService.getBuilder();
        AbstractHttpClientService client = builder.build();

        try (HttpResponse<BufferedReader> rep = runRequest(client, "GET", "/metrics",
                PrometheusTextFormatWriter.CONTENT_TYPE)) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.TEXT_PLAIN, rep.getMimeType());
            Assert.assertTrue(rep.getParsedResponse().lines().count() > 200);
        }
    }

    private HttpResponse<BufferedReader> runRequest(AbstractHttpClientService client, String verb, String path, String accept) {
        HttpRequest<BufferedReader> request = client.getRequest();
        request.setUri(URI.create(String.format("http://localhost:%d/prometheus%s", port, path)));
        request.setVerb(verb);
        request.setConsumeText(BufferedReader::new);
        request.setConsumeBytes(is -> new BufferedReader(new InputStreamReader(is)));
        request.addHeader("Accept", accept);
        return client.doRequest(request);
    }

}
