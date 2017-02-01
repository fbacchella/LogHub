/**
 * 
 */
package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;

/**
 * @author Fabrice Bacchella
 *
 */
public class TestFieldsProcessor {

    @Test
    public void test() throws ProcessorException {
        FieldsProcessor p = new FieldsProcessor() {

            @Override
            public boolean processMessage(Event event, String field,
                    String destination) throws ProcessorException {
                event.put(destination, event.get(field));
                return true;
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
