package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelinePipeStep extends PipeStep {

    private static final Logger logger = LogManager.getLogger();
    private final Pipeline pipeline;

    
    public PipelinePipeStep(Pipeline pipeline) {
        super();
        this.pipeline = pipeline;
    }

    public void start() {
        String threadName = getName();
        Thread inthread = new Thread() {
            @Override
            public void run() {
                try {
                    Event event = queueIn.take();
                    pipeline.inQueue.add(event);
                } catch (InterruptedException e) {
                }
            }
        };
        inthread.setDaemon(true);
        inthread.setName(threadName + "/in");
        inthread.start();
        Thread outthread = new Thread() {
            @Override
            public void run() {
                try {
                    Event event = pipeline.outQueue.take();
                    if( ! queueOut.offer(event) ) {
                        Stats.dropped.incrementAndGet();
                        logger.error("send failed for {}, destination blocked", event);
                    };
                    logger.trace("{} send event {}", () -> queueOut, () -> event);
                } catch (InterruptedException e) {
                }
            }
        };
        inthread.setName(threadName + "/out");
        outthread.setDaemon(true);
        outthread.start();
    }

    @Override
    public void run() {
    }

}
