package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
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
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;

public class TestJfr {

    private static Logger logger;

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

    @Before
    public void record() throws IOException, ParseException {
        jfrFile = tempFolder.newFile().toPath();
        Configuration c = Configuration.getConfiguration("default");
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
    public void testKeep() {
        check(Jfr.DURATION_FORMAT.KEEP,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertTrue(e.get("maxAge") instanceof Duration);
                    }
                });
    }

    @Test
    public void testNanos() {
        Event ev = check(Jfr.DURATION_FORMAT.NANOS,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001000000L, ev.get("timespan"));
    }

    @Test
    public void testMicros() {
        Event ev = check(Jfr.DURATION_FORMAT.MICROS,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001000L, ev.get("timespan"));
    }

    @Test
    public void testMillis() {
        Event ev = check(Jfr.DURATION_FORMAT.MILLIS,
                e -> {
                    if (e.containsKey("maxAge")) {
                        Assert.assertEquals(Long.MAX_VALUE, e.get("maxAge"));
                    }
                });
        Assert.assertEquals(1001L, ev.get("timespan"));
    }

    @Test
    public void testSeconds() {
        Event ev = check(Jfr.DURATION_FORMAT.SECONDS,
                e -> { });
        Assert.assertEquals(1L, ev.get("timespan"));
    }

    @Test
    public void testSecondsFloat() {
        Event ev = check(Jfr.DURATION_FORMAT.SECONDS_FLOAT, e -> { });
        Assert.assertEquals(1.001, ev.get("timespan"));
    }

    private Event check(Jfr.DURATION_FORMAT durationFormat, Consumer<Event> check) {
        Jfr.Builder builder = Jfr.getBuilder();
        builder.setJfrFile(jfrFile.toString());
        builder.setDurationUnit(durationFormat);
        try (Jfr jfr = builder.build()) {
            Assert.assertTrue(jfr.configure(new Properties(new HashMap<>())));
            Event ev = jfr.getStream().filter(e -> {
                check.accept(e);
                return "loghub.receivers.TestJfr.HelloWorld".equals(e.get("eventType"));
            }).findAny().get();
            Assert.assertEquals("LogHub/test/jfr", ev.getMeta("category"));
            return ev;
        }
    }

    @Test
    public void test_loghub_receivers_Journald() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Jfr"
                , BeanChecks.BeanInfo.build("durationUnit", Jfr.DURATION_FORMAT.class)
                , BeanChecks.BeanInfo.build("jfrFile", String.class)
        );
    }

}
