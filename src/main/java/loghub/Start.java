package loghub;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.configuration.Configuration;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class Start extends Thread {


    static public void main(String[] args) {

        //Make it wait on himself to wait forever
        try {
            Thread me = new Start();
            me.setName("LogHub");
            me.start();
            me.join();
        } catch (InterruptedException e) {
        }
    }

    public void run() {
        Context context = ZMQ.context(1);
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        
        Configuration conf = new Configuration(eventQueue, context);
        
        conf.parse("conf/conf.yaml");

        List<PipeStep[]> pipe = conf.getTransformersPipe();
        PipeStream mainPipe = new PipeStream(eventQueue, context, "", pipe);

        for(Sender s: conf.getSenders(mainPipe.getOutEndpoint())) {
            s.start();
        }
        
        for(Receiver r: conf.getReceivers(mainPipe.getInEndpoint())) {
            r.start();
        }
        
        // configuration is not needed any more, don't hold reference to it.
        conf = null;
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        context.term();
    }

}
