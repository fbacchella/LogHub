package loghub.sources;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.RouteParser;
import loghub.Tools;
import loghub.Tools.ProcessingStatus;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Etl;
import loghub.processors.Mapper;

public class TestFileMap {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.source");
    }

    @Test
    public void testSourceLoading() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("sources.conf", false);
        conf.sources.values().forEach(i -> Assert.assertTrue(i.configure(conf)));
        FileMap s1 = (FileMap) conf.sources.get("source1");
        Assert.assertEquals("A", s1.get("1"));
        FileMap s2 = (FileMap) conf.sources.get("source2");
        Assert.assertEquals(1, s2.get("a"));
    }

    @Test
    public void testOne() {
        URL ifpixurl = getClass().getResource("/mapping.csv");
        FileMap.Builder builder = FileMap.getBuilder();
        builder.setMappingFile(ifpixurl.getFile());
        builder.setCsvFormat("RFC4180");
        builder.setKeyName("key");
        builder.setValueName("value");
        FileMap s = builder.build();
        s.configure(null);

        Etl p = Mapper.of(VariablePath.of("type"), s, ConfigurationTools.unWrap("[type]", RouteParser::expression));
        Event e = factory.newEvent();
        e.put("type", "1");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        Event ep = ps.mainQueue.remove();
        Assert.assertEquals("A", ep.get("type"));
    }

    @Test
    public void testJson() {
        URL jsonurl = getClass().getResource("/mapping.json");
        FileMap.Builder builder = FileMap.getBuilder();
        builder.setMappingFile(jsonurl.getFile());
        FileMap s = builder.build();
        s.configure(null);

        Etl p = Mapper.of(VariablePath.of("type"), s, ConfigurationTools.unWrap("[type]", RouteParser::expression));
        Event e = factory.newEvent();
        e.put("type", "a");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        Event ep = ps.mainQueue.remove();
        Assert.assertEquals(1, ep.get("type"));
    }

}
