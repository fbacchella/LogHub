package loghub;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.configuration.Configuration;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestConfigurations {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.PipeStep","loghub.Pipeline", "loghub.configuration.Configuration","loghub.receivers.ZMQ", "loghub.Receiver");
    }

    private Configuration loadConf(String configname) {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        return conf;
    }

    @Test(timeout=1000)
    public void testBuildPipeline() throws IOException, InterruptedException {
        Configuration conf = loadConf("simple.conf");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();

        Event sent = new Event();
        eventQueue.put(sent.key(), sent);

        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        Pipeline main = conf.namedPipeLine.get("main");
        Socket in = tctxt.ctx.newSocket(Method.CONNECT, Type.PUSH, main.inEndpoint);
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.PULL, main.outEndpoint);
        in.send(sent.key());
        for(String sockName: tctxt.ctx.getSocketsList()) {
            logger.debug("    " + sockName);
        }

        byte[] buffer = tctxt.ctx.recv(out);
        Event received = eventQueue.get(buffer);
        Assert.assertEquals("not expected event received", sent, received);
        for(String sockName: tctxt.ctx.getSocketsList()) {
            logger.debug("    " + sockName);
        }
        tctxt.ctx.close(in);
        tctxt.ctx.close(out);
        tctxt.terminate();
    }

    @Test(timeout=1000) 
    public void testSimpleInput() throws InterruptedException {
        Configuration conf = loadConf("simpleinput.conf");
        logger.debug("pipelines: {}", conf.pipelines);
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();

        Pipeline main = conf.namedPipeLine.get("main");
        logger.debug("receiver pipelines: {}", conf.getReceiversPipelines());
        //for(String inpipe: conf.getReceiversPipelines()) {
        //    Pipeline in = conf.namedPipeLine.get(inpipe);
        //    ZMQManager.proxy(inpipe, new SocketInfo(Method.CONNECT, Type.PULL, in.outEndpoint), new SocketInfo(Method.CONNECT, Type.PUSH, main.inEndpoint));
        //}
        //Socket temp = ZMQManager.newSocket(Method.CONNECT, Type.PULL, conf.);
        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }
        for(Receiver r: conf.getReceivers()) {
            r.start(eventQueue);
        }
        Thread.sleep(30);
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.PULL, main.outEndpoint);
        Socket sender1 = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1");
        Socket sender2 = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener2");
        Thread.sleep(30);
        sender1.send("something1");
        sender2.send("something1");
        Thread.sleep(30);
        sender1.send("something2");
        sender2.send("something2");
        byte[] buffer = out.recv();
        Event ev = eventQueue.get(buffer);
        Assert.assertNotNull("Event not found", ev);
        tctxt.ctx.close(sender1);
        tctxt.ctx.close(sender2);
        tctxt.ctx.close(out);
        for(Receiver r: conf.getReceivers()) {
            r.interrupt();
        }
        SmartContext.terminate();
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

}
