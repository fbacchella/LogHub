package loghub.processors;

import java.io.IOException;
import java.util.Collection;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.processors.ParseJson;
import loghub.Processor;
import loghub.ProcessorException;

public class TestToJson {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.transformers.Script");
    }

    @Test
    public void test1() throws ProcessorException {
        Event e = Tools.getEvent();
        Processor t = new ParseJson();
        e.put("message", "{\"a\": [ 1, 2.0 , 3.01 , {\"b\": true} ] }");
        e.process(t);
        @SuppressWarnings("unchecked")
        Collection<Object> a = (Collection<Object>) e.get("a");
        a.stream().forEach((i) -> logger.debug(i.getClass()));
        logger.debug(e);
    }
}
