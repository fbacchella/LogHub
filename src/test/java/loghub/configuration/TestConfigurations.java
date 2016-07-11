package loghub.configuration;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.Tools;

public class TestConfigurations {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.Pipeline", "loghub.configuration","loghub.receivers.ZMQ", "loghub.Receiver", "loghub.processors.Forker", "loghub", "loghub.EventsProcessor");
    }

    @Test(timeout=2000)
    public void testBuildPipeline() throws IOException, InterruptedException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        conf.mainQueue.add(sent);
        Event received = conf.mainQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test(timeout=2000)
    public void testBuildSubPipeline() throws IOException, InterruptedException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();

        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        conf.mainQueue.add(sent);
        Event received = conf.mainQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test(timeout=2000) 
    public void testTwoPipe() throws InterruptedException {
        Properties conf = Tools.loadConf("twopipe.conf");
        Thread t = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine);
        t.setDaemon(true);
        t.start();

        Event sent = Tools.getEvent();
        sent.put("message", "1");
        sent.inject(conf.namedPipeLine.get("pipeone"), conf.mainQueue);

        try {
            Event re = conf.outputQueues.get("main").take();
            Assert.assertEquals("wrong event received", "1", re.get("message"));
        } finally {
            logger.debug("{} is at main pipeline {}", sent, sent.getCurrentPipeline());
            t.interrupt();
        }
    }

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
        Properties conf = Tools.loadConf("test.conf", false);
        for(String plName: new String[]{"main", "oneref", "groovy"}) {
            Assert.assertTrue("pipeline '" + plName +"'not found", conf.namedPipeLine.containsKey(plName));
        }
        Assert.assertEquals("input not found", 1, conf.receivers.size());
        Assert.assertEquals("ouput not found", 1, conf.senders.size());
    }

    @Test
    public void testArray() {
        Tools.loadConf("array.conf", false);
    }

}
