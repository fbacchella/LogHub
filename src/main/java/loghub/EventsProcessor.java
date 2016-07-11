package loghub;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        try {
            while (true) {
                Event event = inQueue.take();
                logger.trace("received {}", event);
                try {
                    Processor processor;
                    while ((processor = event.next()) != null) {
                        logger.trace("processing {}", processor);
                        Thread.currentThread().setName(threadName + "-" + processor.getName());
                        process(event, processor);
                        Thread.currentThread().setName(threadName);
                        if(event.dropped) {
                            logger.debug("dropped event {}", event);
                            break;
                        }
                    }
                    //No processor, processing finished
                    //Detect if will send to another pipeline, or just wait for a sender to take it
                    if (processor == null) {
                        if (event.getNextPipeline() != null) {
                            event.inject(namedPipelines.get(event.getNextPipeline()), inQueue);
                        } else {
                            outQueues.get(event.getCurrentPipeline()).put(event);
                        }
                    }
                } catch (ProcessorException e) {
                    logger.error("failed to process event {}: {}", event, e);
                } finally {
                    Thread.currentThread().setName(threadName);
                } 
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void process(Event e, Processor p) throws ProcessorException {
        boolean success = false;
        if (p instanceof Forker) {
            ((Forker) p).fork(e);
            success = true;
        } else if (p instanceof Drop) {
            e.dropped = true;
        } else {
            if (p.isprocessNeeded(e)) {
                try {
                    e.process(p);
                    success = true;
                } catch (ProcessorException ex) {
                    Stats.newError(ex);
                } catch (Exception ex) {
                    logger.error("failed to transform event {} with unmanaged error: {}", e, ex.getMessage());
                    logger.throwing(Level.ERROR, ex);
                    e.dropped = true;
                }
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
    }

}
