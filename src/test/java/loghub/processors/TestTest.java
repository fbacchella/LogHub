package loghub.processors;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Configuration;

public class TestTest {

    private Configuration loadConf(String configname) {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        return conf;
    }

    @Test
    public void testOK() throws InterruptedException {
        Configuration conf = loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = Tools.getEvent();
        sent.put("a",1);

        conf.namedPipeLine.get("main").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("main").outQueue.take();
        Assert.assertEquals("conversion not expected", 1, received.get("b"));
    }
    
    @Test
    public void testKO() throws InterruptedException {
        Configuration conf = loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = Tools.getEvent();
        sent.put("a",2);

        conf.namedPipeLine.get("main").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("main").outQueue.take();
        Assert.assertEquals("conversion not expected", 2, received.get("c"));
    }
    
    @Test
    public void testSub() throws InterruptedException {
        Configuration conf = loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = Tools.getEvent();
        sent.put("a",2);

        conf.namedPipeLine.get("subpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("subpipe").outQueue.take();
        Assert.assertEquals("conversion not expected", 2, received.get("d"));
    }

}
