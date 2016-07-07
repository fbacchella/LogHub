package loghub.processors;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.Tools;
import loghub.configuration.Configuration;

public class TestConditions {

    @Test(timeout=2000)
    public void testif() throws InterruptedException {
        Configuration conf = Tools.loadConf("conditions.conf");

        Event sent = new Event();
        sent.put("a", "1");

        conf.namedPipeLine.get("ifpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("ifpipe").outQueue.take();
        Assert.assertEquals("conversion not expected", String.class, received.get("a").getClass());
    }

    @Test(timeout=2000)
    public void testsuccess() throws InterruptedException {
        Configuration conf = Tools.loadConf("conditions.conf");

        Event sent = new Event();
        sent.put("a", "1");

        conf.namedPipeLine.get("successpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("successpipe").outQueue.take();
        Assert.assertEquals("conversion not expected", "success", received.get("test"));
    }

    @Test(timeout=2000)
    public void testfailure() throws InterruptedException {
        Configuration conf = Tools.loadConf("conditions.conf");

        Event sent = new Event();
        sent.put("a", "a");

        conf.namedPipeLine.get("failurepipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("failurepipe").outQueue.take();
        Assert.assertEquals("conversion not expected", "failure", received.get("test"));
    }

    @Test(timeout=2000)
    public void testsubpipe() throws InterruptedException {
        Configuration conf = Tools.loadConf("conditions.conf");

        Event sent = new Event();
        sent.put("a", "1");

        conf.namedPipeLine.get("subpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("subpipe").outQueue.take();
        System.out.println(received);
        Assert.assertEquals("conversion not expected", "failure", received.get("test"));
    }

}
