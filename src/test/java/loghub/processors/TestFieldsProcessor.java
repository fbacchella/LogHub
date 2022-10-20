/**
 * 
 */
package loghub.processors;

import java.io.IOException;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.events.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.UncheckedProcessorException;
import loghub.VarFormatter;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

/**
 * @author Fabrice Bacchella
 *
 */
public class TestFieldsProcessor {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

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

        p.setDestinationTemplate(new VarFormatter("${field}_done"));
        p.setFields(new String[] {"a", "b"});
        Event e = factory.newEvent();
        e.put("a", 1);
        e.put("b", 2);
        Tools.runProcessing(e, "main", Collections.singletonList(p));
        Assert.assertEquals("destination field wrong", 1, e.get("a_done"));
        Assert.assertEquals("destination field wrong", 2, e.get("b_done"));
    }

    @Test
    public void testFailing() throws ProcessorException {
        Stats.reset();
        FieldsProcessor p = new FieldsProcessor() {

            @Override
            public Object fieldFunction(Event event, Object valuedestination) throws ProcessorException {
                throw event.buildException("Expected error");
            }

            @Override
            public String getName() {
                return null;
            }

        };

        p.setDestinationTemplate(new VarFormatter("${field}_done"));
        p.setFields(new String[] {"a", "b"});
        Event e = factory.newEvent();
        e.put("a", 1);
        e.put("b", 2);
        Tools.runProcessing(e, "main", Collections.singletonList(p));
        long found = Stats.getErrors().stream()
                                      .map( i-> (Throwable) i)
                                      .map(Throwable::getMessage)
                                      .filter( i -> Pattern.matches("Field with path \"\\[.\\]\" invalid: Expected error", i))
                                      .count();
        Assert.assertEquals(1, found);
    }

    @Test
    public void testFailingUnchecked() throws ProcessorException {
        Stats.reset();
        FieldsProcessor p = new FieldsProcessor() {

            @Override
            public Object fieldFunction(Event event, Object valuedestination) throws ProcessorException {
                throw new UncheckedProcessorException(event.buildException("Expected unchecked error"));
            }

            @Override
            public String getName() {
                return null;
            }

        };

        p.setDestinationTemplate(new VarFormatter("${field}_done"));
        p.setFields(new String[] {"a", "b"});
        Event e = factory.newEvent();
        e.put("a", 1);
        e.put("b", 2);
        Tools.runProcessing(e, "main", Collections.singletonList(p));
        long found = Stats.getErrors().stream()
                                      .map( i-> (Throwable) i)
                                      .map(Throwable::getMessage)
                                      .filter( i -> Pattern.matches("Field with path \"\\[.\\]\" invalid: Expected unchecked error", i))
                                       .count();
        Assert.assertEquals(1, found);
    }

}
