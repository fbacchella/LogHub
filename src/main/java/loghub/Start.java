package loghub;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.configuration.Configuration;
import loghub.transformers.Pipe;

public class Start extends Thread {

    private String configFile = null;
    
    static public void main(final String[] args) {

        //Make it wait on himself to wait forever
        try {
            new Start(args[0]) {{
                setName("LogHub");
                start();
                join();
            }};
        } catch (InterruptedException e) {
        }
    }
    
    Start(String configFile) {
        this.configFile = configFile;
    }
    
    public void run() {
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        
        Configuration conf = new Configuration();
        
        conf.parse(configFile);

        for(Map.Entry<String, List<Pipe>> e: conf.getTransformersPipe()) {
            int i = 0;
            for(Pipe p: e.getValue()) {
                p.startStream(eventQueue, e.getKey() + "." + i++);
                System.out.println(p.getInEndpoint() + "->" + p.getOutEndpoint());
            }
        }
//
//        for(Sender s: conf.getSenders(mainPipe.getOutEndpoint())) {
//            s.start();
//        }
//        
//        for(Receiver r: conf.getReceivers(mainPipe.getInEndpoint())) {
//            r.start();
//        }
//        
        // configuration is not needed any more, don't hold reference to it.
        conf = null;
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ZMQManager.terminate();
    }

}
