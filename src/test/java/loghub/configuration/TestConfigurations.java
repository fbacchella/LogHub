package loghub.configuration;

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

import loghub.ContextRule;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.Sender;
import loghub.SmartContext;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Configuration.PipeJoin;
import zmq.ZMQHelper;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.SocketInfo;
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

        logger.debug("receiver pipelines: {}", conf.getReceiversPipelines());
        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }
        Thread.sleep(30);
        for(Receiver r: conf.getReceivers()) {
            r.start(eventQueue);
        }
        Thread.sleep(30);
        for(Sender s: conf.getSenders()) {
            s.start(eventQueue);
        }
        Thread.sleep(30);
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender");
        out.subscribe(new byte[]{});
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1");
        Thread.sleep(30);
        sender.send("something");
        byte[] buffer = out.recv();
        Assert.assertEquals("wrong send message", "something", new String(buffer));
        Assert.assertEquals("Event queue not empty", 0, eventQueue.size());
        tctxt.ctx.close(sender);
        tctxt.ctx.close(out);
        for(Receiver r: conf.getReceivers()) {
            r.interrupt();
        }
        for(Sender s: conf.getSenders()) {
            s.interrupt();
        }
        SmartContext.terminate();
    }

    @Test(timeout=1000) 
    public void testTwoPipe() throws InterruptedException {
        Configuration conf = loadConf("twopipe.conf");
        logger.debug("pipelines: {}", conf.pipelines);
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();

        logger.debug("receiver pipelines: {}", conf.getReceiversPipelines());
        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }
        Thread.sleep(30);
        for(Receiver r: conf.getReceivers()) {
            r.start(eventQueue);
        }
        Thread.sleep(30);
        for(Sender s: conf.getSenders()) {
            s.start(eventQueue);
        }
        Thread.sleep(30);
        for(PipeJoin j: conf.joins) {
            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
            SocketInfo inSi = new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PULL, inpipe.outEndpoint);
            SocketInfo outSi = new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PUSH, outpipe.inEndpoint);
            SmartContext.getContext().proxy(j.toString(), inSi, outSi);
        }
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender");
        out.subscribe(new byte[]{});
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1");
        Thread.sleep(30);
        sender.send("something");
        byte[] buffer = out.recv();
        Assert.assertEquals("wrong send message", "something", new String(buffer));
        Assert.assertEquals("Event queue not empty", 0, eventQueue.size());
        tctxt.ctx.close(sender);
        tctxt.ctx.close(out);
        for(Receiver r: conf.getReceivers()) {
            r.interrupt();
        }
        for(Sender s: conf.getSenders()) {
            s.interrupt();
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
