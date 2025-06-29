package loghub.commands;

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.beust.jcommander.JCommander;

import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestTester {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG);
    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test(timeout = 10000)
    public void testOne() throws IOException {
        Path confFile = folder.newFile("test.conf").toPath();
        try (Writer confWriter = Files.newBufferedWriter(confFile, StandardCharsets.UTF_8)) {
            confWriter.append("pipeline[test1] {[a] = 1} | $test2 pipeline[test2] {[b] = 1}");
        }

        Event ev1 = factory.newTestEvent();
        ev1.put("j", 1);
        Event ev2 = factory.newTestEvent();
        ev2.put("j", 2);

        TestPipeline tester = new TestPipeline();
        Launch launch = new Launch();
        BaseParameters base = new BaseParameters();
        JCommander jcom = JCommander.newBuilder().acceptUnknownOptions(false).addObject(base).addCommand(tester).addObject(launch).build();
        jcom.parse("-c", confFile.toString(), "test", "-p", "test1");
        tester.extractFields(launch);
        Properties props = Tools.loadConf(new StringReader("pipeline[test1] {[a] = 1} | $test2 pipeline[test2] {[b] = 1} numWorkers: 1"));
        List<Event> events = tester.process(props, Stream.of(ev1, ev2)).collect(Collectors.toList());

        try {
            events.forEach(e -> {
                        Assert.assertEquals(1, e.get("a"));
                        Assert.assertFalse(events.contains("b"));
                    }
            );
            Assert.assertEquals(1, events.get(0).get("j"));
            Assert.assertEquals(2, events.get(1).get("j"));
        } finally {
            try {
                for (EventsProcessor ep : props.eventsprocessors) {
                    ep.stopProcessing();
                    ep.join(100);
                }
            } catch (InterruptedException ex) {
                // Ignore, just stop waiting
            }
        }
    }

}
