package loghub;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer.Context;

import loghub.configuration.Properties;
import loghub.processors.Drop;
import loghub.processors.Forker;

public class EventsProcessor extends Thread {

    private final static Logger logger = LogManager.getLogger();

    private final BlockingQueue<Event> inQueue;
    private final Map<String, BlockingQueue<Event>> outQueues;
    private final Map<String,Pipeline> namedPipelines;

    public EventsProcessor(BlockingQueue<Event> inQueue, Map<String, BlockingQueue<Event>> outQueues, Map<String,Pipeline> namedPipelines) {
        super();
        this.inQueue = inQueue;
        this.outQueues = outQueues;
        this.namedPipelines = namedPipelines;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        Counter gaugecounter = null;
        Event event = null;
        try {
            while (true) {
                event = inQueue.take();
                gaugecounter = Properties.metrics.counter("Pipeline." + event.getCurrentPipeline() + ".inflight");
                gaugecounter.inc();
                try (Context timer = Properties.metrics.timer("Pipeline." + event.getCurrentPipeline() + ".timer").time()){
                    logger.trace("received {}", event);
                    Processor processor;
                    while ((processor = event.next()) != null) {
                        logger.trace("processing {}", processor);
                        Thread.currentThread().setName(threadName + "-" + processor.getName());
                        boolean dropped = process(event, processor);
                        Thread.currentThread().setName(threadName);
                        if(dropped) {
                            gaugecounter.dec();
                            logger.debug("dropped event {}", event);
                            Properties.metrics.meter("Allevents.dropped");
                            event.end();
                            event = null;
                            break;
                        }
                    }
                    //No processor, processing finished
                    //Detect if will send to another pipeline, or just wait for a sender to take it
                    if(processor == null) {
                        gaugecounter.dec();
                        gaugecounter = null;
                        if(event.getNextPipeline() != null) {
                            // Send to another pipeline, loop in the main processing queue
                            Pipeline next = namedPipelines.get(event.getNextPipeline());
                            if(! event.inject(next, inQueue)) {
                                Properties.metrics.meter("Pipeline." + next.getName() + ".blocked").mark();
                                event.end();
                                event = null;
                            }
                        } else {
                            // Put in the output queue, where the wanting output will come to take it
                            if(!outQueues.get(event.getCurrentPipeline()).offer(event)) {
                                Properties.metrics.meter("Pipeline." + event.getCurrentPipeline() + ".out.blocked").mark();
                                event.end();
                                event = null;
                            }
                        }
                    } 
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().setName(threadName);
            Thread.currentThread().interrupt();
        }
        if (gaugecounter != null) {
            gaugecounter.dec();
            gaugecounter = null;
        }
        if (event != null) {
            event.end();
        }
    }

    boolean process(Event e, Processor p) {
        boolean dropped = false;
        boolean success = false;
        if (p instanceof Forker) {
            ((Forker) p).fork(e);
            success = true;
        } else if (p instanceof Drop) {
            dropped = true;
        } else {
            try {
                if (p.isprocessNeeded(e)) {
                    e.process(p);
                    success = true;
                }
            } catch (ProcessorException ex) {
                Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".failure").inc();;
                Stats.newError(ex);
            } catch (Exception ex) {
                Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".exception").inc();
                Stats.newException(ex);
                logger.error("failed to transform event {} with unmanaged error: {}", e, ex.getMessage());
                logger.throwing(Level.DEBUG, ex);
                dropped = true;
            }
        }
        // After processing, check the failures and success processors
        Processor failureProcessor = p.getFailure();
        Processor successProcessor = p.getSuccess();
        if (success && successProcessor != null) {
            e.insertProcessor(successProcessor);
        } else if (! success && failureProcessor != null) {
            e.insertProcessor(failureProcessor);
        }
        return dropped;
    }

}
