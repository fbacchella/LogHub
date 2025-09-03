package loghub.metrics;

import java.io.IOException;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static org.junit.Assert.assertEquals;

public class TestJmxStats {

    private final EventsFactory factory = new EventsFactory();
    private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.metrics");
    }

    @Before
    public void start() {
        Stats.reset();
    }

    @After
    public void stop() {
        JmxService.stop();
    }

    @Test
    public void test1() throws NotCompliantMBeanException {
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new NullPointerException();
        Stats.newUnhandledException(e);
        assertEquals(String.format("NullPointerException at loghub.metrics.TestJmxStats.test1 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
        assertEquals(1, Stats.getExceptionsCount());
    }

    @Test
    public void test2() throws NotCompliantMBeanException {
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new RuntimeException("some message");
        Stats.newUnhandledException(e);
        assertEquals(String.format("some message at loghub.metrics.TestJmxStats.test2 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
        assertEquals(1, Stats.getExceptionsCount());
    }

    @Test
    public void testCount()
            throws IOException, ReflectionException, MalformedObjectNameException,
                           AttributeNotFoundException, InstanceNotFoundException, IntrospectionException,
                           MBeanException {
        Properties p = getProperties("pipeline[main] { [a] == 1 ? drop}");

        Event ev = factory.newEvent();
        try {
            ev.end();
            ev.end();
        } catch (AssertionError e) {
            // normal
        }
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        ev.putAtPath(VariablePath.of("a"), 1);
        try {
            Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        } catch (AssertionError e) {
            // normal
        }
        Map<String, Map<String, Object>> attributes = dumpBeans();
        Assert.assertEquals(returnStatMetric(Timer.class, Stats.METRIC_ALL_TIMER).getCount(), attributes.get("Global").remove("TotalEvents"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_EVENT_DUPLICATEEND).getCount(), attributes.get("Global").remove("DuplicateEnd"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_INFLIGHT).getCount(), attributes.get("Global").remove("Inflight"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_EVENT_LEAKED).getCount(), attributes.get("Global").remove("Leaked"));
        Assert.assertEquals(returnStatMetric(Meter.class, Stats.METRIC_ALL_EXCEPTION).getCount(), attributes.get("Global").remove("UnhandledExceptions"));
        Assert.assertNotNull(attributes.get("Global").remove("EventLifeTime95"));
        Assert.assertNotNull(attributes.get("Global").remove("EventLifeTimeMedian"));
        Assert.assertTrue(attributes.get("Global").isEmpty());

        Assert.assertEquals(returnPipelineMetric(Timer.class, Stats.METRIC_PIPELINE_TIMER).getCount(), attributes.get("Pipelines").remove("Count"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_DISCARDED).getCount(), attributes.get("Pipelines").remove("Discarded"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_DROPPED).getCount(), attributes.get("Pipelines").remove("Dropped"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_EXCEPTION).getCount(), attributes.get("Pipelines").remove("Exceptions"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_FAILED).getCount(), attributes.get("Pipelines").remove("Failed"));
        Assert.assertEquals(returnPipelineMetric(Counter.class, Stats.METRIC_PIPELINE_INFLIGHT).getCount(), attributes.get("Pipelines").remove("Inflight"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_LOOPOVERFLOW).getCount(), attributes.get("Pipelines").remove("LoopOverflow"));

        Assert.assertNotNull(attributes.get("Pipelines").remove("95per"));
        Assert.assertNotNull(attributes.get("Pipelines").remove("Median"));

        Assert.assertTrue(attributes.get("Pipelines").isEmpty());
    }

    private <T extends Metric> T returnStatMetric(Class<T> clazz, String name) {
        return Stats.getMetric(Stats.class, name, clazz);
    }

    private <T extends Metric> T returnPipelineMetric(Class<T> clazz, String name) {
        return Stats.getMetric(String.class, name, clazz);
    }

    @Test
    public void testBeans()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException,
                           AttributeNotFoundException, MBeanException, IOException {
        getProperties("pipeline[main] { }");

        Map<String, List<String>> attributes = new HashMap<>();
        for (String type : List.of("Global", "Exceptions", "Pipelines", "Receivers", "Senders")) {
            ObjectName on = ObjectName.getInstance("loghub", "type", type);
            MBeanInfo info = mbs.getMBeanInfo(on);
            for (MBeanAttributeInfo ai : info.getAttributes()) {
                Assert.assertNotNull(on + "." + ai.getName(), ai.getDescriptor().getFieldValue("units"));
                Assert.assertNotNull(on + "." + ai.getName(), ai.getDescriptor().getFieldValue("metricType"));
            }
            List<String> attrList = Arrays.stream(info.getAttributes())
                                          .map(MBeanFeatureInfo::getName)
                                          .sorted()
                                          .collect(Collectors.toList());
            attributes.put(type, attrList);
            for (String a : attrList) {
                Object o = mbs.getAttribute(on, a);
                if (o instanceof Number) {
                    Assert.assertEquals(0, ((Number) o).longValue());
                } else if (o.getClass().isArray()) {
                    Assert.assertEquals(0, Array.getLength(o));
                } else {
                    Assert.fail("Unexpected type: " + o);
                }
            }
        }
        Assert.assertEquals(List.of("DuplicateEnd", "EventLifeTime95", "EventLifeTimeMedian", "Inflight", "Leaked", "TotalEvents", "UnhandledExceptions"), attributes.get("Global"));
        Assert.assertEquals(List.of("DecodersFailures", "ProcessorsFailures", "ReceiversFailures", "SendersFailures", "UnhandledExceptions"), attributes.get("Exceptions"));
        Assert.assertEquals(List.of("95per", "Count", "Discarded", "Dropped", "Exceptions", "Failed", "Inflight", "LoopOverflow", "Median"), attributes.get("Pipelines"));
        Assert.assertEquals(List.of("Blocked", "Bytes", "Count", "Exceptions", "Failed", "FailedDecode"), attributes.get("Receivers"));
        Assert.assertEquals(List.of("ActiveBatches", "Bytes", "Count", "DoneBatches", "Errors", "Exceptions", "Failed", "FlushDuration95", "FlushDurationMedian", "QueueSize", "WaitingBatches"), attributes.get("Senders"));
    }

    private Map<String, Map<String, Object>> dumpBeans()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
                           IntrospectionException, AttributeNotFoundException, MBeanException {

        Map<String, Map<String, Object>> attributes = new HashMap<>();
        for (String type : List.of("Global", "Exceptions", "Pipelines", "Receivers", "Senders")) {
            ObjectName on = ObjectName.getInstance("loghub", "type", type);
            MBeanInfo info = mbs.getMBeanInfo(on);
            List<String> attrList = Arrays.stream(info.getAttributes())
                                          .map(MBeanFeatureInfo::getName).sorted()
                                          .collect(Collectors.toList());
            for (String a : attrList) {
                Object o = mbs.getAttribute(on, a);
                attributes.computeIfAbsent(type, k -> new HashMap<>()).put(a, o);
            }
        }
        return attributes;
    }

    private Properties getProperties(String props) throws IOException {
        Properties p = Configuration.parse(new StringReader(props));
        JmxService.start(p.jmxServiceConfiguration);
        return p;
    }

}
