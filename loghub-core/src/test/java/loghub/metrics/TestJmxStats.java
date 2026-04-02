package loghub.metrics;

import java.io.IOException;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import loghub.LogUtils;
import loghub.Tools;
import loghub.Tools.SimplifiedMbean;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static loghub.Tools.testMBean;

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
    public void testCount() throws IOException, JMException {
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
        Map<String, SimplifiedMbean> attributes = dumpBeans();
        SimplifiedMbean global = attributes.get("loghub:type=Global");
        Assert.assertEquals(returnStatMetric(Timer.class, Stats.METRIC_ALL_TIMER).getCount(), global.values().get("TotalEvents"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_EVENT_DUPLICATEEND).getCount(), global.values().get("DuplicateEnd"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_INFLIGHT).getCount(), global.values().get("Inflight"));
        Assert.assertEquals(returnStatMetric(Counter.class, Stats.METRIC_ALL_EVENT_LEAKED).getCount(), global.values().get("Leaked"));
        Assert.assertEquals(returnStatMetric(Meter.class, Stats.METRIC_ALL_EXCEPTION).getCount(), global.values().get("UnhandledExceptions"));
        Assert.assertEquals(returnStatMetric(Gauge.class, Stats.METRIC_ALL_WAITINGPROCESSING).getValue(), global.values().get("WaitingProcessing"));
        Assert.assertNotNull(global.values().get("EventLifeTime95"));
        Assert.assertNotNull(global.values().get("EventLifeTimeMedian"));

        SimplifiedMbean pipelines = attributes.get("loghub:type=Pipelines");
        Assert.assertEquals(returnPipelineMetric(Timer.class, Stats.METRIC_PIPELINE_TIMER).getCount(), pipelines.values().get("Count"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_DISCARDED).getCount(), pipelines.values().get("Discarded"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_DROPPED).getCount(), pipelines.values().get("Dropped"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_EXCEPTION).getCount(), pipelines.values().get("Exceptions"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_FAILED).getCount(), pipelines.values().get("Failed"));
        Assert.assertEquals(returnPipelineMetric(Counter.class, Stats.METRIC_PIPELINE_INFLIGHT).getCount(), pipelines.values().get("Inflight"));
        Assert.assertEquals(returnPipelineMetric(Meter.class, Stats.METRIC_PIPELINE_LOOPOVERFLOW).getCount(), pipelines.values().get("LoopOverflow"));

        Assert.assertNotNull(pipelines.values().get("95per"));
        Assert.assertNotNull(pipelines.values().get("Median"));

        for (String name: List.of("exception", "discarded", "failed", "loopOverflow", "paused", "pausedCount", "dropped", "inflight", "timer")) {
            String on = "loghub:type=Pipelines,servicename=main,name=%s".formatted(name);
            Assert.assertEquals(on, mbs.getObjectInstance(new ObjectName(on)).getObjectName().toString());
        }
    }

    private <T extends Metric> T returnStatMetric(Class<T> clazz, String name) {
        return Stats.getMetric(Stats.class, name, clazz);
    }

    private <T extends Metric> T returnPipelineMetric(Class<T> clazz, String name) {
        return Stats.getMetric(String.class, name, clazz);
    }

    @Test
    public void testBeans() throws JMException, IOException {
        getProperties("""
            input {
                loghub.receivers.Http {
                    port: 0,
                    decoders: {
                        "application/json": loghub.decoders.Json
                    },
                },
            } | $main
            pipeline[main] { }
            http.port: 0
            """);

        Map<String, Collection<String>> attributes = new HashMap<>();
        for (String type : List.of("Global", "Exceptions", "Pipelines", "Receivers", "Senders")) {
            SimplifiedMbean smb = Tools.testMBean(mbs, "loghub:type=%s".formatted(type));
            ObjectName on = ObjectName.getInstance("loghub", "type", type);
            MBeanInfo info = mbs.getMBeanInfo(on);
            for (MBeanAttributeInfo ai : info.getAttributes()) {
                Assert.assertNotNull(on + "." + ai.getName(), ai.getDescriptor().getFieldValue("units"));
                Assert.assertNotNull(on + "." + ai.getName(), ai.getDescriptor().getFieldValue("metricType"));
            }
            Collection<String> attrList = smb.values().keySet();
            attributes.put(type, attrList);
            for (Object o: smb.values().values()) {
                if (o instanceof Number) {
                    Assert.assertEquals(0, ((Number) o).longValue());
                } else if (o.getClass().isArray()) {
                    Assert.assertEquals(0, Array.getLength(o));
                } else {
                    Assert.fail("Unexpected type: " + o);
                }
            }
        }
        Assert.assertEquals(Set.of("DuplicateEnd", "EventLifeTime95", "EventLifeTimeMedian", "Inflight", "Leaked", "TotalEvents", "UnhandledExceptions", "WaitingProcessing"), attributes.remove("Global"));
        Assert.assertEquals(Set.of("DecodersFailures", "ProcessorsFailures", "ReceiversFailures", "SendersFailures", "UnhandledExceptions"), attributes.remove("Exceptions"));
        Assert.assertEquals(Set.of("95per", "Count", "Discarded", "Dropped", "Exceptions", "Failed", "Inflight", "LoopOverflow", "Median"), attributes.remove("Pipelines"));
        Assert.assertEquals(Set.of("Blocked", "Bytes", "Count", "Exceptions", "Failed", "FailedDecode"), attributes.remove("Receivers"));
        Assert.assertEquals(Set.of("ActiveBatches", "Bytes", "Count", "DoneBatches", "Errors", "Exceptions", "Failed", "FlushDuration95", "FlushDurationMedian", "QueueSize", "WaitingBatches"), attributes.remove("Senders"));
        Assert.assertTrue(attributes.isEmpty());
        for (String code : List.of("200", "301","302", "400", "401", "403", "404", "500", "503")) {
            SimplifiedMbean smb = Tools.testMBean(mbs, "loghub:type=Dashboard,level=HTTPStatus,code=%s".formatted(code));
            Assert.assertTrue(smb.values().containsKey("95thPercentile"));
            Assert.assertTrue(smb.values().containsKey("DurationUnit"));
            Assert.assertTrue(smb.values().containsKey("Mean"));
        }
        testMBean(mbs, "loghub:type=Receivers,servicename=HTTP/0.0.0.0/0,level=HTTPStatus,code=200");
    }

    private Map<String, SimplifiedMbean> dumpBeans() throws JMException {
        Map<String, SimplifiedMbean> attributes = new HashMap<>();
        for (String type : List.of("Global", "Exceptions", "Pipelines", "Receivers", "Senders")) {
            SimplifiedMbean smb = Tools.testMBean(mbs, "loghub:type=%s".formatted(type));
            attributes.put(smb.name().toString(), smb);
        }
        return attributes;
    }

    private Properties getProperties(String props) throws IOException {
        Properties p = Configuration.parse(new StringReader(props));
        JmxService.start(p.jmxServiceConfiguration);
        return p;
    }

}
