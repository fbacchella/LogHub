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

    @Test(timeout=2000)
    public void testBuildPipeline() throws IOException, InterruptedException {
        Configuration conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        sent.inject(conf.properties.mainQueue);
        Event received = conf.properties.mainQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test(timeout=2000)
    public void testBuildSubPipeline() throws IOException, InterruptedException {
        Configuration conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();

        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        Pipeline parent = conf.namedPipeLine.get("parent");
        sent.inject(conf.properties.mainQueue);
        Event received = conf.properties.mainQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

//    @Test(timeout=2000) 
//    public void testTwoPipe() throws InterruptedException {
//        Configuration conf = Tools.loadConf("twopipe.conf");
//        Set<Thread> joins = new HashSet<>();
//        int i = 0;
//        for(PipeJoin j: conf.joins) {
//            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
//            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
//            Thread t = Helpers.QueueProxy("join-" + i++, inpipe.outQueue, outpipe.inQueue, () -> { System.out.println("error");});
//            t.start();
//            joins.add(t);
//        }
//        Event sent = Tools.getEvent();
//        sent.put("message", "1");
//        conf.namedPipeLine.get("pipeone").inQueue.add(sent);
//        Event re = conf.namedPipeLine.get("main").outQueue.take();
//        Assert.assertEquals("wrong event received", "1", re.get("message"));
//    }
//
//    @Test(timeout=2000)
//    public void testFork() throws InterruptedException {
//        Configuration conf = Tools.loadConf("fork.conf");
//
//        Event sent = Tools.getEvent();
//        sent.put("childs", new HashMap<String, Object>());
//
//        conf.namedPipeLine.get("main").inQueue.offer(sent);
//        conf.namedPipeLine.get("main").outQueue.take();
//        conf.namedPipeLine.get("forked").outQueue.take();
//    }

    @Test
    public void testComplexConf() {
        Configuration conf = Tools.loadConf("test.conf", false);
        for(String plName: new String[]{"main", "oneref", "groovy"}) {
            Assert.assertTrue("pipeline '" + plName +"'not found", conf.namedPipeLine.containsKey(plName));
        }
        Assert.assertEquals("input not found", 1, conf.getReceivers().size());
        Assert.assertEquals("ouput not found", 1, conf.getSenders().size());
    }

    @Test
    public void testArray() {
        Tools.loadConf("array.conf", false);
    }

}
