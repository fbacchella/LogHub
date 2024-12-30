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
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import loghub.metrics.JmxService;

public class TestDashboard extends AbstractDashboard {

    @BeforeClass
    public static void configure() throws IOException {
        JmxService.start(getProps().jmxServiceConfiguration);
    }

    @AfterClass
    public static void stopJmx() {
        JmxService.stop();
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
    public void getMetricGlobal() throws IOException, IntrospectionException, InstanceNotFoundException,
                                                 MalformedObjectNameException, ReflectionException {
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

    private void checkMetric(String path) throws IOException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        URL theurl = URI.create(String.format("%s://localhost:%d/graph/%s", getScheme(), getPort(), path)).toURL();
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(false);
        Assert.assertEquals(301, cnx.getResponseCode());
        Assert.assertEquals("/static/index.html?q=%2F" + path, cnx.getHeaderField("location"));

        theurl = URI.create(String.format("%s://localhost:%d/metric/%s", getScheme(), getPort(), path)).toURL();
        TypeReference<List<Map<String, String>>> tr = new TypeReference<>() { };
        List<Map<String, String>> data = getJson().get().readValue(theurl, tr);
        for (Map<String, String> m : data) {
            String on = m.get("url").replace("/jmx/", "");
            Assert.assertNotNull(server.getMBeanInfo(new ObjectName(on)));
        }
    }

}
