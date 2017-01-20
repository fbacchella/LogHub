package loghub;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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

    private static class ContextWrapper implements Closeable, AutoCloseable {
        private final Context timer;
        ContextWrapper(Context timer) {
            this.timer = timer;
        }
        @Override
        public
        void close() {
            if (timer !=null) {
                timer.close();
            }
        }
    }

    private final BlockingQueue<Event> inQueue;
    private final Map<String, BlockingQueue<Event>> outQueues;
    private final Map<String,Pipeline> namedPipelines;
    private final int maxSteps;

    public EventsProcessor(BlockingQueue<Event> inQueue, Map<String, BlockingQueue<Event>> outQueues, Map<String,Pipeline> namedPipelines, int maxSteps) {
        super();
        this.inQueue = inQueue;
        this.outQueues = outQueues;
        this.namedPipelines = namedPipelines;
        this.maxSteps = maxSteps;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        final AtomicReference<Counter> gaugecounter = new AtomicReference<>();
        Event event = null;
        try {
            while (true) {
                event = inQueue.take();
                if (!event.isTest()) {
                    gaugecounter.set(Properties.metrics.counter("Pipeline." + event.getCurrentPipeline() + ".inflight"));
                    gaugecounter.get().inc();
                }
                try (ContextWrapper cw = new ContextWrapper(event.isTest() ? null : Properties.metrics.timer("Pipeline." + event.getCurrentPipeline() + ".timer").time())){
                    logger.trace("received {}", event);
                    Processor processor;
                    while ((processor = event.next()) != null) {
                        logger.trace("processing {}", processor);
                        Thread.currentThread().setName(threadName + "-" + processor.getName());
                        boolean dropped = process(event, processor);
                        Thread.currentThread().setName(threadName);
                        if (dropped) {
                            logger.debug("dropped event {}", event);
                            event.doMetric(() -> {
                                gaugecounter.get().dec();
                                Properties.metrics.meter("Allevents.dropped");
                            });
                            event.drop();
                            event = null;
                            break;
                        }
                    }
                    //No processor, processing finished
                    //Detect if will send to another pipeline, or just wait for a sender to take it
                    if (processor == null) {
                        event.doMetric(() -> {
                            gaugecounter.get().dec();
                            gaugecounter.set(null);
                        });
                        // A test event, it will not be send to another pipeline
                        if (event.isTest()) {
                            logger.info("processed a test event: {}", event);
                            event.end();
                        } else if (event.getNextPipeline() != null) {
                            // Send to another pipeline, loop in the main processing queue
                            Pipeline next = namedPipelines.get(event.getNextPipeline());
                            if (! event.inject(next, inQueue)) {
                                Properties.metrics.meter("Pipeline." + next.getName() + ".blocked").mark();
                                event.end();
                                event = null;
                            }
                        } else if (event.getCurrentPipeline() != null){
                            // Put in the output queue, where the wanting output will come to take it
                            if (!outQueues.get(event.getCurrentPipeline()).offer(event)) {
                                final String currentPipeline = event.getCurrentPipeline();
                                Properties.metrics.meter("Pipeline." + currentPipeline + ".out.blocked").mark();
                                event.end();
                                event = null;
                            }
                        } else {
                            logger.error("Miss-configured event droped: {}", event);
                            event.drop();
                            event = null;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().setName(threadName);
            Thread.currentThread().interrupt();
        }
        if (gaugecounter.get() != null) {
            gaugecounter.get().dec();
            gaugecounter.set(null);
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
        } else if (e.stepsCount() > maxSteps) {
            logger.error("Too much steps for event {}, dropping", e);
            dropped = true;
        } else {
            try {
                if (p.isprocessNeeded(e)) {
                    success = e.process(p);
                }
                // After processing, check the failures and success processors
                Processor failureProcessor = p.getFailure();
                Processor successProcessor = p.getSuccess();
                if (success && successProcessor != null) {
                    e.insertProcessor(successProcessor);
                } else if (! success && failureProcessor != null) {
                    e.insertProcessor(failureProcessor);
                }
            } catch (ProcessorException.DroppedEventException ex) {
                dropped = true;
            } catch (ProcessorException ex) {
                e.doMetric(() -> {
                    Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".failure").inc();
                    Stats.newError(ex);
                });
                Processor exceptionProcessor = p.getException();
                if (exceptionProcessor != null) {
                    e.insertProcessor(exceptionProcessor);
                }
            } catch (Exception ex) {
                e.doMetric(() -> {
                    Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".exception").inc();
                    Stats.newException(ex);
                });
                String message;
                if (ex instanceof NullPointerException) {
                    message = "NullPointerException";
                } else {
                    message = ex.getMessage();
                }
                logger.error("failed to transform event {} with unmanaged error: {}", e, message);
                logger.throwing(Level.DEBUG, ex);
                dropped = true;
            }
        }
        return dropped;
    }

}
