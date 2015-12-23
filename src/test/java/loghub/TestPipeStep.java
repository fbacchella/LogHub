package loghub;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.ZMQManager.Method;
import loghub.ZMQManager.Type;
import loghub.transformers.Identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class TestPipeStep {

    private static final Logger logger = LogManager.getLogger();

    @Test(timeout=5000)
    public void testPipeStep() {
        PipeStep ps = new PipeStep(1, 1);
        ps.addTransformer(new Identity());
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        eventQueue.put("totor".getBytes(), new Event());

        String inEndpoint = "inproc://in." + "TestPipeStep";
        String outEndpoint = "inproc://out." + "TestPipeStep";

        //  Socket facing clients
        Socket in = ZMQManager.newSocket(Method.CONNECT, Type.PUSH, inEndpoint);

        //  Socket facing services
        Socket out = ZMQManager.newSocket(Method.CONNECT, Type.PULL, outEndpoint);

        try {
            ps.start(eventQueue, inEndpoint, outEndpoint);
        } catch (Exception e) {
            Throwable t = e;
            do {
                t.printStackTrace();
            } while((t = t.getCause()) != null);
        }
        in.send(eventQueue.keySet().iterator().next());
        System.out.println(out.recvStr());
        ZMQManager.close(in);
        ZMQManager.close(out);
        try {
            ZMQManager.terminate();
        } catch (Exception e) {
            Throwable t = e;
            do {
                t.printStackTrace();
            } while((t = t.getCause()) != null);
        }
    }

    @Test(timeout=5000)
    public void testPipeline() {

        PipeStep subps = new PipeStep();
        subps.addTransformer(new Identity() {

            @Override
            public void transform(Event event) {
                logger.debug("step 1");
            }
            
        });
        subps.addTransformer(new Identity() {

            @Override
            public void transform(Event event) {
                logger.debug("step 2");
            }
            
        });
        Pipeline pipeline = new Pipeline(Collections.singletonList(new PipeStep[] {subps}), "main");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        eventQueue.put("1".getBytes(), new Event());
        pipeline.startStream(eventQueue);

        Socket in = ZMQManager.newSocket(Method.CONNECT, Type.PUSH, pipeline.inEndpoint);
        Socket out = ZMQManager.newSocket(Method.CONNECT, Type.PULL, pipeline.outEndpoint);

        logger.debug("pipeline is " + pipeline);
        for(String s: ZMQManager.getSocketsList()) {
            logger.debug("sockets: " + s); 
        }
        logger.debug("send message: " + eventQueue.keySet().iterator().next());
        in.send(eventQueue.keySet().iterator().next());
        ZMsg msg = ZMsg.recvMsg(out);
        logger.debug("received message: " + msg);
        int i = 0;
        while(msg.size() > 0) {
            ZFrame frame = msg.pop();
            logger.debug("received frame " + i++ + " " + frame.toString());
        }
        msg.destroy();

        ZMQManager.close(in);
        ZMQManager.close(out);
        try {
            ZMQManager.terminate();
        } catch (Exception e) {
            Throwable t = e;
            do {
                t.printStackTrace();
            } while((t = t.getCause()) != null);
        }
    }

     public void testSub() {
        System.out.println("testSub");
        final String subInEndpoint = "inproc://in." + "subTestPipeStep";
        final String subOutEndpoint = "inproc://out." + "subTestPipeStep";

        PipeStep subps = new PipeStep();
        subps.addTransformer(new Identity());
        Pipeline pipeline = new Pipeline(Collections.singletonList(new PipeStep[] {subps}), "subTestPipeStep");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        eventQueue.put("totor".getBytes(), new Event());
        pipeline.startStream(eventQueue);
        
        PipeStep ps = pipeline.getPipeSteps()[0];

        String inEndpoint = "inproc://in." + "TestPipeStep";
        String outEndpoint = "inproc://out." + "TestPipeStep";

        Socket in = ZMQManager.newSocket(Method.BIND, Type.PUSH, inEndpoint);
        Socket out = ZMQManager.newSocket(Method.BIND, Type.PULL, outEndpoint);

        Thread thread = new Thread() {
            final Socket subIn;
            final Socket subOut;

            {
                //  Socket facing clients
                subIn = ZMQManager.newSocket(Method.BIND, Type.PULL, subInEndpoint);

                //  Socket facing services
                subOut = ZMQManager.newSocket(Method.BIND, Type.PUSH, subOutEndpoint);

                setDaemon(true);
                start();
            }
            @Override
            public void run() {
                try {
                    subOut.send(subIn.recv());
                    System.out.println("ressent");                        
                    //                    ZMsg msg = ZMsg.recvMsg(subIn);
                    //                    System.out.println("received from SubPipeline" + msg);
                    //                    ZFrame part = null;
                    //                    while(msg.size() > 1) {
                    //                        System.out.println("part " + part);
                    //                        msg.removeFirst().sendAndDestroy(subOut, ZFrame.REUSE + ZFrame.MORE);
                    //                    }
                    //                    msg.removeFirst().sendAndDestroy(subOut, ZFrame.REUSE);
                    //                    msg.destroy();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ZMQManager.close(subIn);
                ZMQManager.close(subOut);
            };
        };
        ps.start(eventQueue, inEndpoint, outEndpoint);
        logger.debug(pipeline);
        for(String s: ZMQManager.getSocketsList()) {
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
        ZMQManager.close(in);
        ZMQManager.close(out);
        try {
            ZMQManager.terminate();
        } catch (Exception e) {
            Throwable t = e;
            do {
                t.printStackTrace();
            } while((t = t.getCause()) != null);
        }
    }

}
