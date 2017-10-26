package loghub.processors;

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

public class TestExternalMapping {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression", "loghub.EventsProcessor");
    }

    @Test
    public void testcsv() throws ProcessorException {
        URL ifpixurl = getClass().getResource("/ipfix-information-elements.csv");
        ExternalMap processor = new ExternalMap();
        processor.setMappingFile(ifpixurl.getFile());
        processor.setCsvFormat("RFC4180");
        processor.setKeyName("ElementID");
        processor.setValueName("Name");
        processor.setField("type");

        Event e = Tools.getEvent();
        e.put("type", "1");

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(processor));
        Event ep = ps.mainQueue.remove();
        Assert.assertEquals("octetDeltaCount", ep.get("type"));
    }

}
