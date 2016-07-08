package loghub.configuration;

import java.io.IOException;

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
import loghub.LogUtils;
import loghub.Receiver;
import loghub.Sender;
import loghub.SmartContext;
import loghub.Tools;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestWithZMQ {

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

    @Ignore
    @Test(timeout=1000) 
    public void testSimpleInput() throws InterruptedException {
        Configuration conf = loadConf("simpleinput.conf");
        logger.debug("pipelines: {}", conf.pipelines);

        logger.debug("receiver pipelines: {}", conf.inputpipelines);
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

}
