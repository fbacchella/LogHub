package loghub.processors;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.EventWrapper;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class TestEtl {

    @Test
    public void test1() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue("a.b");
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
        Etl etl = new Etl.Remove();
        etl.setLvalue("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", null, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a"}, null, false));
    }

    @Test
    public void test3() throws ProcessorException {
        Etl.Rename etl = new Etl.Rename();
        etl.setLvalue("b");
        etl.setSource("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = new Event();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 0, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"b"}, null, false));
    }

    @Test
    public void test4() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue("a");
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

    @Test(timeout=2000)
    public void test5() throws ProcessorException, InterruptedException {
        String conffile = getClass().getClassLoader().getResource("etl.conf").getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        for(Pipeline pipe: conf.pipelines) {
            System.out.println(pipe);
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Event sent = new Event();
        sent.put("a", "a");

        conf.namedPipeLine.get("main").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("main").outQueue.take();
        Assert.assertEquals("conversion not expected", "a", received.get("a"));
    }

    @Test(timeout=2000)
    public void test6() throws ProcessorException, InterruptedException {
        String conffile = getClass().getClassLoader().getResource("etl.conf").getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Event sent = new Event();
        sent.put("count", "1");

        conf.namedPipeLine.get("second").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("second").outQueue.take();
        Assert.assertEquals("conversion not expected", 1, received.get("count"));

    }

}
