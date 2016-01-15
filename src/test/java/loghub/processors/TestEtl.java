package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.configuration.Etl;
import loghub.configuration.Properties;

public class TestEtl {

    @Test
    public void test1() {
        Etl etl = new Etl();
        etl.setLvalue("a.b");
        etl.setOperator('=');
        etl.setExpression("[c] + 1");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("c", 0);
        etl.process(event);
        System.out.println(event);
        Assert.assertEquals("evaluation failed", 1, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a", "b"}, null, false));
    }

    @Test
    public void test2() {
        Etl etl = new Etl();
        etl.setLvalue("a");
        etl.setOperator('-');
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        System.out.println(event);
        Assert.assertEquals("evaluation failed", null, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a"}, null, false));
    }

    @Test
    public void test3() {
        Etl etl = new Etl();
        etl.setLvalue("b");
        etl.setOperator('<');
        etl.setExpression("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        System.out.println(event);
        Assert.assertEquals("evaluation failed", 0, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"b"}, null, false));
    }

}
