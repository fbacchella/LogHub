package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import jdk.jfr.Category;
import jdk.jfr.Configuration;
import jdk.jfr.Name;
import jdk.jfr.Recording;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;
import loghub.BeanChecks;
import loghub.DurationUnit;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.JmxService;

public class TestJfr {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    public Path jfrFile;

    @Category({"LogHub", "test", "jfr"})
    @StackTrace(false)
    @Name("loghub.receivers.TestJfr.HelloWorld")
    static class HelloWorld extends jdk.jfr.Event {
        String message;
        @Timespan(Timespan.MILLISECONDS)
        public long timespan;
    }

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Jfr");
    }

    //@Before
    public void startRecording() throws IOException, ParseException {
        jfrFile = tempFolder.newFile().toPath();
        Configuration c = Configuration.getConfiguration("profile");
        try (Recording r = new Recording(c)) {
            r.start();
            HelloWorld event = new HelloWorld();
            event.message = "LogHub test";
            event.timespan = 1001;
            event.commit();
            r.stop();
            r.dump(jfrFile);
        }
    }

    @Test
    public void testKeep() throws IOException, ParseException {
        check(DurationUnit.DURATION,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertTrue(e.get("maxAge") instanceof Duration);
                    }
                });
    }

    @Test
    public void testNanos() throws IOException, ParseException {
        Event ev = check(DurationUnit.NANO,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001000000L, ev.get("timespan"));
    }

    @Test
    public void testMicros() throws IOException, ParseException {
        Event ev = check(DurationUnit.MICRO,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001000L, ev.get("timespan"));
    }

    @Test
    public void testMillis() throws IOException, ParseException {
        Event ev = check(DurationUnit.MILLI,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001L, ev.get("timespan"));
    }

    @Test
    public void testSeconds() throws IOException, ParseException {
        Event ev = check(DurationUnit.SECOND,
                e -> { });
        Assert.assertEquals(1L, ev.get("timespan"));
    }

    @Test
    public void testSecondsFloat() throws IOException, ParseException {
        Event ev = check(DurationUnit.SECOND_FLOAT, e -> { });
        Assert.assertEquals(1.001, ev.get("timespan"));
    }

    private Event check(DurationUnit durationFormat, Consumer<Event> check) throws IOException, ParseException {
        startRecording();
        Jfr.Builder builder = Jfr.getBuilder();
        builder.setJfrFile(jfrFile.toString());
        builder.setDurationUnit(durationFormat);
        builder.setEventsFactory(factory);
        try (Jfr jfr = builder.build()) {
            Assert.assertTrue(jfr.configure(new Properties(new HashMap<>())));
            Event ev = jfr.getStream().filter(e -> {
                check.accept(e);
                return "loghub.receivers.TestJfr.HelloWorld".equals(e.getAtPath(VariablePath.of("eventType", "name")));
            }).findAny().get();
            URI sourceUri = (URI) ev.getConnectionContext().getRemoteAddress();
            Assert.assertEquals("file", sourceUri.getScheme());
            Assert.assertEquals("loghub.receivers.TestJfr.HelloWorld", ev.getAtPath(VariablePath.of("eventType", "name")));
            return ev;
        }
    }

    @Test
    public void testStream() throws IOException, InterruptedException {
        int port = Tools.tryGetPort();
        String confile = """
                jmx.port: %1$d
                jmx.hostname: "127.0.0.1"
                jmx.serviceUrl: "service:jmx:rmi://127.0.0.1:%1$d/jndi/rmi://127.0.0.1:%1$d/jmxrmi"
                input {
                    loghub.receivers.Jfr {
                        jmxUrl: "service:jmx:rmi:///jndi/rmi://127.0.0.1:%1$d/jmxrmi",
                        jfrConfiguration: "default",
                        settings: {
                            "jdk.ActiveSetting#enabled": "false",
                        }
                    }
                } | $main
                pipeline[main] {}
                """.formatted(port);
        Properties conf = Tools.loadConf(new StringReader(confile));
        JmxService.start(conf.jmxServiceConfiguration);
        try {
            conf.receivers.forEach(r -> {
                r.configure(conf);
                r.start();
            });
            HelloWorld event = new HelloWorld();
            event.message = "LogHub test";
            event.timespan = 1001;
            event.commit();
            for (int i = 0; i < 10 ;i++) {
                Event ev = conf.mainQueue.take();
                URI sourceUri = (URI) ev.getConnectionContext().getRemoteAddress();
                Assert.assertEquals("service", sourceUri.getScheme());
            }
        } finally {
            JmxService.stop();
        }
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Jfr"
                , BeanChecks.BeanInfo.build("durationUnit", DurationUnit.class)
                , BeanChecks.BeanInfo.build("jfrFile", String.class)
                , BeanChecks.BeanInfo.build("jmxUrl", String.class)
                , BeanChecks.BeanInfo.build("flushInterval", int.class)
                , BeanChecks.BeanInfo.build("jfrConfiguration", String.class)
                , BeanChecks.BeanInfo.build("jfrConfigurationFile", String.class)
                , BeanChecks.BeanInfo.build("jfrSettings", Map.class)
        );
    }
}
