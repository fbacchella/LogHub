package loghub;

import java.util.concurrent.atomic.AtomicInteger;

import loghub.configuration.Configuration;
import loghub.configuration.Configuration.PipeJoin;

public class Start extends Thread {


    static public void main(final String[] args) {
        new Start(args[0]).start();
    }

    public Start(String configFile) {

        setName("LogHub");

        Configuration conf = new Configuration();

        conf.parse(configFile);

        for(Pipeline pipe: conf.pipelines) {
            if(pipe.configure(conf.properties)) {
                pipe.startStream();
            };
        }

        for(Sender s: conf.getSenders()) {
            if(s.configure(conf.properties)) {
                s.start();
            };
        }

        for(Receiver r: conf.getReceivers()) {
            if(r.configure(conf.properties)) {
                r.start();
            }
        }

        for(PipeJoin j: conf.joins) {
            AtomicInteger i = new AtomicInteger();
            Pipeline inpipe = conf.namedPipeLine.get(j.inpipe);
            Pipeline outpipe = conf.namedPipeLine.get(j.outpipe);
            Helpers.QueueProxy("join-" + i.getAndIncrement(), inpipe.inQueue, outpipe.outQueue, () -> { }).start();
        }

    }

    public void run() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e1) {
        }
        SmartContext.terminate();
    }

}
