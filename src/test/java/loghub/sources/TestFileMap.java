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

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.Tools.ProcessingStatus;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.processors.Mapper;

public class TestFileMap {

    private static Logger logger;

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
        FileMap s = (FileMap) conf.sources.get("source1");
        Assert.assertEquals("Reserved", s.get("0"));
    }

    @Test
    public void testone() throws ProcessorException {
        URL ifpixurl = getClass().getResource("/ipfix-information-elements.csv");
        FileMap s = new FileMap();
        s.setMappingFile(ifpixurl.getFile());
        s.setCsvFormat("RFC4180");
        s.setKeyName("ElementID");
        s.setValueName("Name");
        s.configure(null);

        Mapper p = new Mapper();
        p.setExpression(ConfigurationTools.unWrap("[type]", i -> i.expression()));
        p.setLvalue(new String[] {"type"});
        p.setMap(s);

        Event e = Tools.getEvent();
        e.put("type", "1");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        Event ep = ps.mainQueue.remove();
        Assert.assertEquals("octetDeltaCount", ep.get("type"));
    }

}
