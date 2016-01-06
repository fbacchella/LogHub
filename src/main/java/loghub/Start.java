package loghub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.configuration.Configuration;
import loghub.configuration.Configuration.PipeJoin;
import zmq.ZMQHelper;
import zmq.ZMQHelper.SocketInfo;

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

        for(Pipeline pipe: conf.pipelines) {
            if(pipe.configure(conf.properties)) {
                pipe.startStream(eventQueue);
            };
        }

        for(Sender s: conf.getSenders()) {
            if(s.configure(conf.properties)) {
                s.start(eventQueue);
            };
        }

        for(Receiver r: conf.getReceivers()) {
            if(r.configure(conf.properties)) {
                r.start(eventQueue);
            }
        }

        for(PipeJoin j: conf.joins) {
            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
            SocketInfo inSi = new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PULL, inpipe.outEndpoint);
            SocketInfo outSi = new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PUSH, outpipe.inEndpoint);
            SmartContext.getContext().proxy(j.toString(), inSi, outSi);
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
