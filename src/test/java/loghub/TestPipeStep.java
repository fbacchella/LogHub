package loghub;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;

import loghub.processors.Identity;

import org.zeromq.ZMsg;

import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestPipeStep {

    private static Logger logger ;

    @Rule
    public ContextRule tctxt = new ContextRule();


    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test(timeout=1000)
    public void testPipeStep() throws InterruptedException {
        logger.debug("start test");
        PipeStep ps = new PipeStep("test", 1, 1);
        ps.addProcessor(new Identity());
        ps.addProcessor(new Identity());
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        Event sent = new Event();
        sent.type = "testEvent";
        eventQueue.put(sent.key(), sent);
        System.out.println(sent.key() + " = " + Arrays.toString(sent.key()));

        String inEndpoint = "inproc://in.TestPipeStep";
        String outEndpoint = "inproc://out.TestPipeStep";

        //  Socket facing clients
        Socket in = tctxt.ctx.newSocket(Method.BIND, Type.PUSH, inEndpoint);

        //  Socket facing services
        Socket out = tctxt.ctx.newSocket(Method.BIND, Type.PULL, outEndpoint);

        try {
            ps.start(eventQueue, inEndpoint, outEndpoint);
        } catch (Exception e) {
            Throwable t = e;
            do {
                t.printStackTrace();
            } while((t = t.getCause()) != null);
        }
        in.send(sent.key());
        byte[] keyReceived = out.recv();
        Event received = eventQueue.get(keyReceived);
        Assert.assertEquals("Not expected event received", sent, received);
        tctxt.ctx.close(in);
        tctxt.ctx.close(out);
        System.out.println("LoggerContext.getContext().getName(): " + LoggerContext.getContext().getName());
        System.out.println("LoggerContext.getContext().getLoggers(): "+ LoggerContext.getContext().getLoggers());
        System.out.println("LoggerContext.getContext().getConfiguration().getRootLogger().getLevel(): "+ LoggerContext.getContext().getConfiguration().getRootLogger().getLevel());
    }

    @Test(timeout=1000)
    public void testPipeline() throws InterruptedException {

        PipeStep subps = new PipeStep();
        subps.addProcessor(new Identity() {

            @Override
            public void process(Event event) {
                logger.debug("step 1");
            }

        });
        subps.addProcessor(new Identity() {

            @Override
            public void process(Event event) {
                logger.debug("step 2");
            }

        });
        Pipeline pipeline = new Pipeline(Collections.singletonList(new PipeStep[] {subps}), "main");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        Event sent = new Event();
        sent.type = "testEvent";
        eventQueue.put(sent.key(), sent);
        System.out.println(sent.key() + " = " + Arrays.toString(sent.key()));
        pipeline.startStream(eventQueue);

        Socket in = tctxt.ctx.newSocket(Method.CONNECT, Type.PUSH, pipeline.inEndpoint);
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.PULL, pipeline.outEndpoint);

        logger.debug("pipeline is " + pipeline);
        logger.debug("send message: " + sent.key());
        in.send(sent.key());
        byte[] keyReceived = out.recv();
        Event received = eventQueue.get(keyReceived);
        Assert.assertEquals("Not expected event received", sent, received);

        tctxt.ctx.close(in);
        tctxt.ctx.close(out);
        tctxt.terminate();
    }

    public void testSub() throws InterruptedException {
        System.out.println("testSub");
        final String subInEndpoint = "inproc://in." + "subTestPipeStep";
        final String subOutEndpoint = "inproc://out." + "subTestPipeStep";

        PipeStep subps = new PipeStep();
        subps.addProcessor(new Identity());
        Pipeline pipeline = new Pipeline(Collections.singletonList(new PipeStep[] {subps}), "subTestPipeStep");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        eventQueue.put("1".getBytes(), new Event());
        pipeline.startStream(eventQueue);

        PipeStep ps = pipeline.getPipeSteps()[0];

        String inEndpoint = "inproc://in." + "TestPipeStep";
        String outEndpoint = "inproc://out." + "TestPipeStep";

        Socket in = tctxt.ctx.newSocket(Method.BIND, Type.PUSH, inEndpoint);
        Socket out = tctxt.ctx.newSocket(Method.BIND, Type.PULL, outEndpoint);

        Thread thread = new Thread() {
            final Socket subIn;
            final Socket subOut;

            {
                //  Socket facing clients
                subIn = tctxt.ctx.newSocket(Method.BIND, Type.PULL, subInEndpoint);

                //  Socket facing services
                subOut = tctxt.ctx.newSocket(Method.BIND, Type.PUSH, subOutEndpoint);

                setDaemon(true);
                start();
            }
            @Override
            public void run() {
                try {
                    subOut.send(subIn.recv());
                    System.out.println("ressent");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                tctxt.ctx.close(subIn);
                tctxt.ctx.close(subOut);
            };
        };
        ps.start(eventQueue, inEndpoint, outEndpoint);
        logger.debug(pipeline);
        for(String s: tctxt.ctx.getSocketsList()) {
            logger.debug(s); 
        }
        in.send(eventQueue.keySet().iterator().next());
        ZMsg msg = ZMsg.recvMsg(out);
        System.out.println(msg);
        int i = 0;
        while(msg.size() > 0) {
            ZFrame frame = msg.pop();
            System.out.println("final frame " + i++ + " " + frame.toString());
        }
        msg.destroy();
        try {
            thread.join();
            System.out.println("sub thread finished");
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        tctxt.ctx.close(in);
        tctxt.ctx.close(out);
        tctxt.terminate();
    }

}
