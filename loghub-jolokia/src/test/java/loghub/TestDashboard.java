package loghub;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.configuration.Properties;
import loghub.httpclient.ApacheHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.metrics.JmxService;

public class TestDashboard {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    private final static Properties props = new Properties(Collections.emptyMap());
    private Dashboard dashboard = null;
    private final int port = Tools.tryGetPort();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Dashboard", "loghub.netty", "org.jolokia");
        JmxService.start(props.jmxServiceConfiguration);
    }

    @AfterClass
    static public void stopJmx() {
        JmxService.stop();
    }

    @Before
    public void startDashBoard() throws IllegalArgumentException, InterruptedException {
        dashboard = Dashboard.getBuilder().setPort(port).setListen("localhost").setWithJolokia(true).build();
        dashboard.start();
    }

    @After
    public void stopDashBoard() {
        Optional.ofNullable(dashboard).ifPresent(Dashboard::stop);
    }

    @Test
    public void jolokia() throws IOException {
        ApacheHttpClientService.Builder builder = ApacheHttpClientService.getBuilder();
        builder.setTimeout(10000000);
        ApacheHttpClientService client = builder.build();

        try (HttpResponse<Map<String, ?>> rep = runRequest(client, "GET", "/version", null)) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.APPLICATION_JSON, rep.getMimeType());
            Map<String, ?> body = rep.getParsedResponse();
            Assert.assertEquals(200, body.get("status"));
        }

        String bodypost1 = json.get().writer().writeValueAsString(Map.of("type", "read",
                "mbean", "java.lang:type=Memory",
                "attribute", "HeapMemoryUsage",
                "path", "used"));


        try (HttpResponse<Map<String, ?>> rep = runRequest(client, "POST", "/", bodypost1)) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.APPLICATION_JSON, rep.getMimeType());
            Map<String, ?> body = rep.getParsedResponse();
            Assert.assertEquals(403, body.get("status"));
            Assert.assertTrue(body.containsKey("request"));
            Assert.assertTrue(body.containsKey("error_type"));
        }

        try (HttpResponse<Map<String, ?>> rep = runRequest(client, "GET", "/read/loghub:type=Global/Inflight", "")) {
            Assert.assertEquals(200, rep.getStatus());
            Assert.assertEquals(ContentType.APPLICATION_JSON, rep.getMimeType());
            Map<String, ?> body = rep.getParsedResponse();
            Assert.assertEquals(200, body.get("status"));
            Assert.assertTrue(body.containsKey("request"));
            Assert.assertTrue(body.containsKey("value"));
        }
    }

    private HttpResponse<Map<String, ?>> runRequest(ApacheHttpClientService client, String verb, String path, String bodypost) {
        HttpRequest<Map<String, ?>> request = client.getRequest();
        request.setUri(URI.create(String.format("http://localhost:%d/jolokia%s", port, path)));
        request.setVerb(verb);
        request.setConsumeText(r -> json.get().reader().readValue(r, Map.class));
        if (bodypost != null && ! bodypost.isEmpty()) {
            request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> os.write(bodypost.getBytes(StandardCharsets.UTF_8)));
        }
        return client.doRequest(request);
    }

}