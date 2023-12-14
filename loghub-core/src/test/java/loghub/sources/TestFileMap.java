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
import loghub.ProcessorException;
import loghub.RouteParser;
import loghub.Tools;
import loghub.Tools.ProcessingStatus;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Mapper;

public class TestFileMap {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.source");
    }

    @Test
    public void testSourceLoading() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("sources.conf", false);
        conf.sources.values().forEach(i -> Assert.assertTrue(i.configure(conf)));
        FileMap s1 = (FileMap) conf.sources.get("source1");
        Assert.assertEquals("Reserved", s1.get("0"));
        FileMap s2 = (FileMap) conf.sources.get("source2");
        Assert.assertEquals(1, s2.get("a"));
    }

    @Test
    public void testone() throws ProcessorException {
        URL ifpixurl = getClass().getResource("/ipfix-information-elements.csv");
        FileMap.Builder builder = FileMap.getBuilder();
        builder.setMappingFile(ifpixurl.getFile());
        builder.setCsvFormat("RFC4180");
        builder.setKeyName("ElementID");
        builder.setValueName("Name");
        FileMap s = builder.build();
        s.configure(null);

        Mapper p = new Mapper();
        p.setExpression(ConfigurationTools.unWrap("[type]", RouteParser::expression));
        p.setLvalue(VariablePath.of("type"));
        p.setMap(s);

        Event e = factory.newEvent();
        e.put("type", "1");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        Event ep = ps.mainQueue.poll().get();
        Assert.assertEquals("octetDeltaCount", ep.get("type"));
    }

    @Test
    public void testJson() throws ProcessorException {
        URL jsonurl = getClass().getResource("/mapping.json");
        FileMap.Builder builder = FileMap.getBuilder();
        builder.setMappingFile(jsonurl.getFile());
        FileMap s = builder.build();
        s.configure(null);

        Mapper p = new Mapper();
        p.setExpression(ConfigurationTools.unWrap("[type]", RouteParser::expression));
        p.setLvalue(VariablePath.of("type"));
        p.setMap(s);

        Event e = factory.newEvent();
        e.put("type", "a");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        Event ep = ps.mainQueue.poll().get();
        Assert.assertEquals(1, ep.get("type"));
    }

}
