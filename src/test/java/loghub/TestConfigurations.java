package loghub;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.ZMQManager.Method;
import loghub.ZMQManager.Type;
import loghub.configuration.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

public class TestConfigurations {

    private static final Logger logger = LogManager.getLogger();


    private Configuration loadConf(String configname) {
        String conffile = getClass().getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        return conf;
    }
    

    @Test(timeout=5000)
    public void testSimple() throws IOException {
        Configuration conf = loadConf("simple.conf");
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        eventQueue.put("1".getBytes(), new Event());
    
        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }
        System.out.println(conf.pipelines);
        System.out.println(conf.namedPipeLine);
        Pipeline main = conf.namedPipeLine.get("main");
        Socket in = ZMQManager.newSocket(Method.CONNECT, Type.PUSH, main.getInEndpoint());
        Socket out = ZMQManager.newSocket(Method.CONNECT, Type.PULL, main.getOutEndpoint());
        in.send(eventQueue.keySet().iterator().next());
        for(String sockName: ZMQManager.getSocketsList()) {
            logger.debug("    " + sockName);
        }
        
        byte[] buffer = out.recv();
        Event ev = eventQueue.get(buffer);
        Assert.assertNotNull("Event not found", ev);
        ZMQManager.close(in);
        ZMQManager.close(out);
        ZMQManager.terminate();
    }

}
