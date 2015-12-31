package loghub;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.configuration.Configuration;

public class Start extends Thread {

    private String configFile = null;
    
    static public void main(final String[] args) {

        //Make it wait on himself to wait forever
        try {
            new Start(args[0]) {{
                start();
                join();
            }};
        } catch (InterruptedException e) {
        }
    }
    
    Start(String configFile) {
        this.configFile = configFile;
        setName("LogHub");
    }
    
    public void run() {
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        
        Configuration conf = new Configuration();
        
        conf.parse(configFile);

        for(Map.Entry<String, List<Pipeline>> e: conf.getTransformersPipe()) {
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
            }
        }

        for(Sender s: conf.getSenders()) {
            s.configure(eventQueue);
        }
        
        for(Receiver r: conf.getReceivers()) {
            r.start(eventQueue);
        }
        
        // configuration is not needed any more, don't hold reference to it.
        conf = null;
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e1) {
        }
        SmartContext.terminate();
    }

}
