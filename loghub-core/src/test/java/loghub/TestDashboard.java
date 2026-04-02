package loghub;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import loghub.Tools.SimplifiedMbean;
import loghub.metrics.JmxService;

class TestDashboard extends AbstractDashboard {

    @BeforeAll
    static void configure() throws IOException {
        JmxService.start(getProps().jmxServiceConfiguration);
    }

    @Override
    protected boolean withSsl() {
        return false;
    }

    @Override
    protected String getDashboardScheme() {
        return "http";
    }

    @Test
    void testJmx() throws IOException, JMException {
        new URL(String.format("http://localhost:%d/", getPort())).getContent();
        SimplifiedMbean status200 = Tools.testMBean(ManagementFactory.getPlatformMBeanServer(), "loghub:type=Dashboard,level=HTTPStatus,code=200");
        long count = ((Number)(status200.values().get("Count"))).longValue();
        Assertions.assertTrue(count > 0 && count < 10);
        SimplifiedMbean status301 = Tools.testMBean(ManagementFactory.getPlatformMBeanServer(), "loghub:type=Dashboard,level=HTTPStatus,code=301");
        count = ((Number)(status301.values().get("Count"))).longValue();
        Assertions.assertTrue(count > 0 && count < 10);
    }

    @Test
    void getMetricGlobal() throws IOException, IntrospectionException, InstanceNotFoundException,
                                                 MalformedObjectNameException, ReflectionException {
        checkMetric("global");
    }

    @Test
    void getMetricReceiver() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("receivers");
    }

    @Test
    void getMetricPipeline() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("pipelines");
    }

    @Test
    void getMetricSender() throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        checkMetric("senders");
    }

    private void checkMetric(String path) throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        URL theurl = URI.create(String.format("%s://localhost:%d/graph/%s", getScheme(), getPort(), path)).toURL();
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assertions.assertEquals(301, cnx.getResponseCode());
        Assertions.assertEquals("/static/index.html?q=%2F" + path, cnx.getHeaderField("location"));

        theurl = URI.create(String.format("%s://localhost:%d/metric/%s", getScheme(), getPort(), path)).toURL();
        TypeReference<List<Map<String, String>>> tr = new TypeReference<>() { };
        List<Map<String, String>> data = getJson().get().readValue(theurl.openStream(), tr);
        for (Map<String, String> m : data) {
            String on = m.get("url").replace("/jmx/", "");
            Assertions.assertNotNull(server.getMBeanInfo(new ObjectName(on)));
        }
    }

}
