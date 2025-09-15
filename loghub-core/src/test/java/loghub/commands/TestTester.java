package loghub.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;

public class TestTester {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG);
    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testIterator() throws IOException, DecodeException {
        Path confFile = folder.newFile("test.conf").toPath();
        try (Writer confWriter = Files.newBufferedWriter(confFile, StandardCharsets.UTF_8)) {
            confWriter.append("pipeline[test1] {[a] = 1} | $test2 pipeline[test2] {[b] = 1}");
        }

        List<Path> samples = List.of(folder.newFile("test1.json").toPath(), folder.newFile("test2.json").toPath());
        int count = 0;
        VarFormatter vf = new VarFormatter("${%j}");

        for (Path jsonFile : samples) {
            try (Writer confWriter = Files.newBufferedWriter(jsonFile, StandardCharsets.UTF_8)) {
                for (int i= 0; i < 2; i++) {
                    Event ev = factory.newTestEvent();
                    ev.put("j", count++);
                    confWriter.append(vf.format(ev));
                }
            }
        }
        ObjectReader r =  JacksonBuilder.get(JsonMapper.class).getReader();
        Parser parser = new Parser();
        List<Map<String, Object>> objects;
        JCommander jcom = parser.parse(List.of("-c", confFile.toString(), "test", "-p", "test1", samples.get(0).toString(), samples.get(1).toString()).toArray(String[]::new));
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assert.assertEquals(0, status);
            String output = bos.toString(StandardCharsets.UTF_8);
            try(MappingIterator<Map<String, Object>> iter = r.readValues(output)) {
                objects = iter.readAll();
            }
        }
        Assert.assertEquals(4, objects.size());
        for (int i = 0; i < 4; i++) {
            Event ev = factory.mapToEvent(ConnectionContext.EMPTY, objects.get(i), true);
            Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a")));
            Assert.assertEquals(i, ev.getAtPath(VariablePath.of("j")));
        }
    }

}
