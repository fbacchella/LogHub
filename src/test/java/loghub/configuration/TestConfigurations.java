package loghub.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Configuration.PipeJoin;

public class TestConfigurations {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.PipeStep","loghub.Pipeline", "loghub.configuration.Configuration","loghub.receivers.ZMQ", "loghub.Receiver", "loghub.processors.Forker");
    }

    private Configuration loadConf(String configname) {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        return conf;
    }

    @Test(timeout=2000)
    public void testBuildPipeline() throws IOException, InterruptedException {
        Configuration conf = loadConf("simple.conf");
        Event sent = new Event();

        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        Pipeline main = conf.namedPipeLine.get("main");
        main.inQueue.offer(sent);
        Event received = main.outQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test(timeout=2000)
    public void testBuildSubPipeline() throws IOException, InterruptedException {
        Configuration conf = loadConf("simple.conf");
        Event sent = new Event();

        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        Pipeline parent = conf.namedPipeLine.get("parent");
        parent.inQueue.offer(sent);
        Event received = parent.outQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test(timeout=2000) 
    public void testTwoPipe() throws InterruptedException {
        Configuration conf = loadConf("twopipe.conf");
        logger.debug("pipelines: {}", conf.pipelines);

        logger.debug("receiver pipelines: {}", conf.inputpipelines);
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Set<Thread> joins = new HashSet<>();
        int i = 0;
        for(PipeJoin j: conf.joins) {
            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
            Thread t = Helpers.QueueProxy("join-" + i++, inpipe.outQueue, outpipe.inQueue, () -> { System.out.println("error");});
            t.start();
            joins.add(t);
        }
        Event se = new Event();
        se.put("message", "1");
        conf.namedPipeLine.get("pipeone").inQueue.add(se);
        Event re = conf.namedPipeLine.get("main").outQueue.take();
        Assert.assertEquals("wrong event received", "1", re.get("message"));
    }

    @Test(timeout=2000)
    public void testFork() throws InterruptedException {
        Configuration conf = loadConf("fork.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        logger.debug("pipelines: {}", conf.pipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = new Event();
        sent.put("childs", new HashMap<String, Object>());

        conf.namedPipeLine.get("main").inQueue.offer(sent);
        conf.namedPipeLine.get("main").outQueue.take();
        conf.namedPipeLine.get("forked").outQueue.take();
    }

    @Test
    public void testComplexConf() {
        Configuration conf = loadConf("test.conf");
        for(String plName: new String[]{"main", "oneref", "groovy"}) {
            Assert.assertTrue("pipeline '" + plName +"'not found", conf.namedPipeLine.containsKey(plName));
        }
        Assert.assertEquals("input not found", 1, conf.getReceivers().size());
        Assert.assertEquals("ouput not found", 1, conf.getSenders().size());
    }

    @Test
    public void testArray() {
        loadConf("array.conf");
    }

    @Test(timeout=2000)
    public void testif() throws InterruptedException {
        Configuration conf = loadConf("conditions.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        logger.debug("pipelines: {}", conf.pipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = new Event();
        sent.put("a", "1");

        conf.namedPipeLine.get("ifpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("ifpipe").outQueue.take();
        Assert.assertEquals("conversion not expected", String.class, received.get("a").getClass());
    }

    @Test(timeout=2000)
    public void testsuccess() throws InterruptedException {
        Configuration conf = loadConf("conditions.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        logger.debug("pipelines: {}", conf.pipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = new Event();
        sent.put("a", "1");

        conf.namedPipeLine.get("successpipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("successpipe").outQueue.take();
        Assert.assertEquals("conversion not expected", "success", received.get("test"));
    }

    @Test(timeout=2000)
    public void testfailure() throws InterruptedException {
        Configuration conf = loadConf("conditions.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        logger.debug("pipelines: {}", conf.pipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }

        Event sent = new Event();
        sent.put("a", "a");

        conf.namedPipeLine.get("failurepipe").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("failurepipe").outQueue.take();
        System.out.println(received);
        Assert.assertEquals("conversion not expected", "failure", received.get("test"));
    }

}
