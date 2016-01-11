package loghub.configuration;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.ContextRule;
import loghub.Event;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.Sender;
import loghub.SmartContext;
import loghub.Start;
import loghub.Tools;
import loghub.configuration.Configuration.PipeJoin;
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
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.PipeStep","loghub.Pipeline", "loghub.configuration.Configuration","loghub.receivers.ZMQ", "loghub.Receiver", "loghub.processors.Forker");
    }

    private Configuration loadConf(String configname) {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        return conf;
    }

    private Start createStart(String configname) throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, InstantiationException, IllegalAccessException, IOException, NotBoundException {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        return new Start(conffile);
    }


    @Test(timeout=1000)
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
        for(String sockName: tctxt.ctx.getSocketsList()) {
            logger.debug("    " + sockName);
        }

        Event received = main.outQueue.take();
        Assert.assertEquals("not expected event received", sent, received);
        for(String sockName: tctxt.ctx.getSocketsList()) {
            logger.debug("    " + sockName);
        }
    }

    @Test(timeout=1000) 
    public void testSimpleInput() throws InterruptedException {
        Configuration conf = loadConf("simpleinput.conf");
        logger.debug("pipelines: {}", conf.pipelines);

        logger.debug("receiver pipelines: {}", conf.inputpipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Thread.sleep(30);
        for(Receiver r: conf.getReceivers()) {
            r.start();
        }
        Thread.sleep(30);
        for(Sender s: conf.getSenders()) {
            s.start();
        }
        Thread.sleep(30);
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender", 1, -1);
        out.subscribe(new byte[]{});
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1", 1, -1);
        Thread.sleep(30);
        sender.send("something");
        byte[] buffer = out.recv();
        Assert.assertEquals("wrong send message", "something", new String(buffer));
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

        logger.debug("receiver pipelines: {}", conf.inputpipelines);
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Thread.sleep(30);
        for(Receiver r: conf.getReceivers()) {
            r.start();
        }
        Thread.sleep(30);
        for(Sender s: conf.getSenders()) {
            s.start();
        }
        Thread.sleep(30);
        Set<Thread> joins = new HashSet<>();
        int i = 0;
        for(PipeJoin j: conf.joins) {
            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
            Thread t = Helpers.QueueProxy("test" + i++, inpipe.outQueue, outpipe.inQueue,  () -> {} );
            t.start();
            joins.add(t);
        }
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender", 1, -1);
        out.subscribe(new byte[]{});
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1", 1, -1);
        Thread.sleep(30);
        sender.send("something");
        byte[] buffer = out.recv();
        Assert.assertEquals("wrong send message", "something", new String(buffer));
        tctxt.ctx.close(sender);
        tctxt.ctx.close(out);
        conf.getReceivers().stream().forEach(r -> r.interrupt());
        conf.getSenders().stream().forEach(s -> s.interrupt());
        conf.pipelines.stream().forEach(p -> p.stopStream());
        joins.stream().forEach(t -> t.interrupt());
    }

    @Test(timeout=1000)
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

    @Ignore
    @Test(timeout=1000)
    public void testfill() throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, InstantiationException, IllegalAccessException, IOException, NotBoundException, InterruptedException {
        Start s = createStart("filesbuffer.conf");
        s.start();
        s.join();
    }

}
