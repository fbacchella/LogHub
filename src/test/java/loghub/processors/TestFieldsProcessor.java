/**
 * 
 */
package loghub.processors;

import java.io.IOException;
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

/**
 * @author Fabrice Bacchella
 *
 */
public class TestFieldsProcessor {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.EventsProcessor");
    }

    @Test
    public void test() throws ProcessorException {
        FieldsProcessor p = new FieldsProcessor() {

            @Override
            public Object fieldFunction(Event event, Object valuedestination) throws ProcessorException {
                return valuedestination;
            }

            @Override
            public String getName() {
                return null;
            }

        };

        p.setDestination("${field}_done");
        p.setFields(new String[] {"a", "b"});
        Event e = Tools.getEvent();
        e.put("a", 1);
        e.put("b", 2);
        Tools.runProcessing(e, "main", Collections.singletonList(p));
        Assert.assertEquals("destination field wrong", 1, e.get("a_done"));
        Assert.assertEquals("destination field wrong", 2, e.get("b_done"));
    }

}
