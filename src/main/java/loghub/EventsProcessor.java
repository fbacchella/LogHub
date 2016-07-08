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

    public EventsProcessor(BlockingQueue<Event> inQueue, Map<String, BlockingQueue<Event>> outQueues) {
        super();
        this.inQueue = inQueue;
        this.outQueues = outQueues;
    }


    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        try {
            Event event = inQueue.take();
            try {
                Processor processor;
                while((processor = event.next()) != null) {
                    //Processor processor = event.next();
                    Thread.currentThread().setName(threadName + "-" + processor.getName());
                    process(event, processor);
                    Thread.currentThread().setName(threadName);
                    if(event.dropped) {
                        logger.debug("dropped event {}", () -> event);
                        break;
                    }
                }
                //No processor, processing finished
                if(processor == null) {
                    event.inject(outQueues.get(event.mainPipeline));
                }
            } catch (ProcessorException e) {
                logger.error("failed to process event {}: {}", event, e);
            } finally {
                Thread.currentThread().setName(threadName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(Event e, Processor p) throws ProcessorException {
        boolean success = false;
        if (p instanceof Forker) {
            ((Forker) p).fork(e);
            success = true;
        } else if (p instanceof Drop) {
            e.dropped = true;
        } else {
            if(p.isprocessNeeded(e)) {
                try {
                    p.process(e);
                    success = true;
                } catch (ProcessorException ex) {
                    Stats.newError(ex);
                } catch (Exception ex) {
                    logger.error("failed to transform event with unmanaged error {}: {}", this, ex.getMessage());
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
