package loghub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.configuration.Properties;
import loghub.metrics.JmxService;

public class TestDashboard {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    private static Logger logger;
    private final static Properties props = new Properties(Collections.emptyMap());
    private DashboardHttpServer dashboard = null;
    private final int port = Tools.tryGetPort();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.DashboardHttpServer", "loghub.netty.http");
        JmxService.start(props.jmxServiceConfiguration);
    }

    @After
    public void stopJmx() throws IOException {
        JmxService.stop();
    }

    @Before
    public void startDashBoard() throws IllegalArgumentException, InterruptedException {
        dashboard = DashboardHttpServer.getBuilder().setPort(port).setHost("localhost").setThreadPrefix("TestDashboard").build();
    }

    @After
    public void stopDashBoard() {
        Optional.ofNullable(dashboard).ifPresent(DashboardHttpServer::close);
    }

    @Test
    public void getIndex() throws IllegalArgumentException, InterruptedException, IOException {
        URL theurl = new URL(String.format("http://localhost:%d/static/index.html", port));
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(200, cnx.getResponseCode());
        String page = readContent(cnx);
        Assert.assertTrue(page.startsWith("<!DOCTYPE html>"));
        cnx.disconnect();
    }

    @Test
    public void getMetricGlobal() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("global");
    }

    @Test
    public void getMetricReceiver() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("receivers");
    }

    @Test
    public void getMetricPipeline() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("pipelines");
    }

    @Test
    public void getMetricSender() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("senders");
    }

    @Test
    public void getFailure1() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        URL theurl = new URL(String.format("http://localhost:%d/metric/1", port));
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(400, cnx.getResponseCode());
        Assert.assertEquals("Unsupported metric name: 1", readErrorContent(cnx));
        cnx.disconnect();
    }

    @Test
    public void getFailure2() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        URL theurl = new URL(String.format("http://localhost:%d/metric/stranges", port));
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(400, cnx.getResponseCode());
        Assert.assertEquals("Unsupported metric name: stranges", readErrorContent(cnx));
        cnx.disconnect();
    }

    private String readContent(HttpURLConnection cnx) throws IOException {
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

    private List<Map<String, String>> checkMetric(String path) throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        URL theurl = new URL(String.format("http://localhost:%d/graph/" + path, port));
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(301, cnx.getResponseCode());
        Assert.assertEquals("/static/index.html?q=%2F" + path, cnx.getHeaderField("location"));

        theurl = new URL(String.format("http://localhost:%d/metric/" + path, port));
        TypeReference<List<Map<String, String>>> tr = new TypeReference<List<Map<String, String>>>() { };
        List<Map<String, String>> data = json.get().readValue(theurl, tr);
        for(Map<String, String> m: data) {
            String on = m.get("url").replace("/jmx/", "");
            Assert.assertNotNull(server.getMBeanInfo(new ObjectName(on)));
        }
        return data;
    }
}
