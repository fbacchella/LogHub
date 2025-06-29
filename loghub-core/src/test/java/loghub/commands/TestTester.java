package loghub.commands;

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import loghub.VarFormatter;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestTester {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.pipeline.vault_audit");
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
        JCommander jcom = JCommander.newBuilder().acceptUnknownOptions(false).addObject(base).addCommand(tester)
                                  .addObject(launch).build();
        jcom.parse("-c", confFile.toString(), "test", "-p", "test1");
        tester.extractFields(launch);
        Properties props = Tools.loadConf(
                new StringReader("pipeline[test1] {[a] = 1} | $test2 pipeline[test2] {[b] = 1} numWorkers: 1"));
        List<Event> events = tester.process(props, Stream.of(ev1, ev2)).collect(Collectors.toList());

        try {
            events.forEach(e -> {
                Assert.assertEquals(1, e.get("a"));
                Assert.assertFalse(e.containsKey("b"));
            });
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

    @Test
    public void testIterator() throws IOException {
        VarFormatter vf = new VarFormatter("${%j}");
        List<Path> samples = List.of(folder.newFile("test1.json").toPath(), folder.newFile("test2.json").toPath());
        int count = 0;
        for (Path jsonFile : samples) {
            try (Writer confWriter = Files.newBufferedWriter(jsonFile, StandardCharsets.UTF_8)) {
                for (int i= 0; i < 2; i++) {
                    Event ev = factory.newTestEvent();
                    ev.put("j", count++);
                    confWriter.append(vf.format(ev));
                }
            }
        }
        TestPipeline tester = new TestPipeline();
        Iterator<Event> ie = new TestPipeline.EventProducer(
                samples.stream().map(Path::toString).collect(Collectors.toList()),
                tester.mapper
        );
        List<Event> events = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(ie, 0),
                false
        ).collect(Collectors.toList());
        for (int i = 0 ; i < 3 ; i++) {
            Assert.assertEquals(i, events.get(i).get("j"));
        }
    }

}
