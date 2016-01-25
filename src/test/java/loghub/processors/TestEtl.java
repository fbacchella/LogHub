package loghub.processors;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.EventWrapper;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class TestEtl {

    @Test
    public void test1() throws ProcessorException {
        Etl etl = new Etl();
        etl.setLvalue("a.b");
        etl.setOperator('=');
        etl.setExpression("event.c + 1");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("c", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 1, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a", "b"}, null, false));
    }

    @Test
    public void test2() throws ProcessorException {
        Etl etl = new Etl();
        etl.setLvalue("a");
        etl.setOperator('-');
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", null, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a"}, null, false));
    }

    @Test
    public void test3() throws ProcessorException {
        Etl etl = new Etl();
        etl.setLvalue("b");
        etl.setOperator('<');
        etl.setExpression("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 0, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"b"}, null, false));
    }

    @Test
    public void test4() throws ProcessorException {
        Etl etl = new Etl();
        etl.setLvalue("a");
        etl.setOperator('=');
        etl.setExpression("formatters.a.format(event)");
        Map<String, String> formats = Collections.singletonMap("a", "${@timestamp%t<GMT>H}");
        Map<String, Object> properties = new HashMap<>();
        properties.put("__formatters", formats);
        boolean done = etl.configure(new Properties(properties));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.timestamp = new Date(3600 * 1000);
        EventWrapper ew = new EventWrapper(event);
        ew.setProcessor(etl);
        etl.process(ew);
        Assert.assertEquals("evaluation failed", "01", event.get("a"));
    }

}
