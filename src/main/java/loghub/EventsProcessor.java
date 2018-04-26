package loghub;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;

import io.netty.util.concurrent.Future;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.processors.Drop;
import loghub.processors.Forker;
import loghub.processors.Forwarder;
import loghub.processors.FuturProcessor;
import loghub.processors.UnwrapEvent;
import loghub.processors.WrapEvent;

public class EventsProcessor extends Thread {

    enum ProcessingStatus {
        // Does not mean the processing return true
        // It just mean it reached it's end
        SUCCESS,
        PAUSED,
        DROPED,
        FAILED
    }

    private static final Logger logger = LogManager.getLogger();
    private static final AtomicInteger id = new AtomicInteger();

    private final BlockingQueue<Event> inQueue;
    private final Map<String, BlockingQueue<Event>> outQueues;
    private final Map<String,Pipeline> namedPipelines;
    private final int maxSteps;
    private final EventsRepository<Future<?>> evrepo;

    public EventsProcessor(BlockingQueue<Event> inQueue, Map<String, BlockingQueue<Event>> outQueues, Map<String,Pipeline> namedPipelines, int maxSteps, EventsRepository<Future<?>> evrepo) {
        this.inQueue = inQueue;
        this.outQueues = outQueues;
        this.namedPipelines = namedPipelines;
        this.maxSteps = maxSteps;
        this.evrepo = evrepo;
        setName("EventsProcessor/" + id.getAndIncrement());
        setDaemon(false);
    }

    @Override
    public void run() {
        final AtomicReference<Counter> gaugecounter = new AtomicReference<>();
        while (! isInterrupted()) {
            Event event = null;
            try {
                event = inQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            { // Needed because eventtemp must be final
                final Event eventtemp  = event;
                event.doMetric(() -> {
                    gaugecounter.set(Properties.metrics.counter("Pipeline." + eventtemp.getCurrentPipeline() + ".inflight"));
                    gaugecounter.get().inc();
                });
            }
            { // Needed because eventtemp must be final
                final Event eventtemp  = event;
                logger.trace("received {} in {}", () -> eventtemp, () -> eventtemp.getCurrentPipeline());
            }
            Processor processor = event.next();
            while (processor != null) {
                logger.trace("processing with {}", processor);
                if (processor instanceof WrapEvent) {
                    event = new EventWrapper(event, processor.getPathArray());
                } else if (processor instanceof UnwrapEvent) {
                    event = event.unwrap();
                } else {
                    ProcessingStatus processingstatus = process(event, processor);
                    if (processingstatus != ProcessingStatus.SUCCESS) {
                        event.doMetric(() -> {
                            gaugecounter.get().dec();
                            gaugecounter.set(null);
                        });
                        // Processing status was non null, so the event will not be processed any more
                        // But it's needed to check why.
                        String currentPipeline = event.getCurrentPipeline();
                        switch (processingstatus) {
                        case DROPED: {
                            //It was a drop action
                            logger.debug("dropped event {}", event);
                            event.doMetric(() -> {
                                Properties.metrics.meter("Allevents.dropped");
                                Properties.metrics.meter("Pipeline." + currentPipeline + ".dropped").mark();
                            });
                            event.drop();
                            break;
                        }
                        case FAILED: {
                            //Processing failed critically (with an exception) and no recovery was attempted
                            logger.debug("Failed event {}", event);
                            event.doMetric(() -> {
                                Properties.metrics.meter("Allevents.failed");
                                Properties.metrics.meter("Pipeline." + currentPipeline + ".failed").mark();
                            });
                            event.end();
                            break;
                        }
                        case PAUSED:
                            //It's simply a paused event, nothing to worry
                            break;
                        case SUCCESS:
                            // Unreachable code
                            break;
                        }
                        // It was not a success, end the processing.
                        event = null;
                        break;
                    }
                }
                processor = event.next();
                // If next processor is null, refill the event
                while (processor == null && event.getNextPipeline() != null) {
                    // Send to another pipeline, loop in the main processing queue
                    Pipeline next = namedPipelines.get(event.getNextPipeline());
                    event.refill(next);
                    processor = event.next();
                }
            }
            // Processing of the event is finished, what to do next with it ?
            //Detect if will send to another pipeline, or just wait for a sender to take it
            if (event != null) {
                event.doMetric(() -> {
                    gaugecounter.get().dec();
                    gaugecounter.set(null);
                });
                if (event.isTest()) {
                    // A test event, it will not be send an output queue
                    // Checked after pipeline forwarding, but before output sending
                    TestEventProcessing.log(event);
                    event.end();
                } else if (event.getCurrentPipeline() != null && outQueues.containsKey(event.getCurrentPipeline())){
                    // Put in the output queue, where the wanting output will come to take it
                    try {
                        outQueues.get(event.getCurrentPipeline()).put(event);
                    } catch (InterruptedException e) {
                        Stats.dropped.incrementAndGet();
                        event.end();
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.error("Miss-configured event droped: {}", event);
                    Properties.metrics.meter("Allevents.failed");
                    event.end();
                }
            }
        }
    }

    ProcessingStatus process(Event e, Processor p) {
        ProcessingStatus status = null;
        if (p instanceof Forker) {
            if (((Forker) p).fork(e)) {
                status = ProcessingStatus.SUCCESS;
            } else {
                status = ProcessingStatus.FAILED;
            }
        } else if (p instanceof Forwarder) {
            ((Forwarder) p).forward(e);
            status = ProcessingStatus.SUCCESS;
        } else if (p instanceof Drop) {
            status = ProcessingStatus.DROPED;
        } else if (e.stepsCount() > maxSteps) {
            logger.error("Too much steps for event {}, dropping", e);
            status = ProcessingStatus.FAILED;
        } else {
            try {
                boolean success = false;
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
                status = ProcessingStatus.SUCCESS;
            } catch (ProcessorException.PausedEventException ex) {
                // The event to pause might be a transformation of the original event.
                Event topause = ex.getEvent();
                // First check if the process will be able to manage the call back
                if (p instanceof AsyncProcessor) {
                    AsyncProcessor<?> ap = (AsyncProcessor<?>) p;
                    // A paused event was catch, create a custom FuturProcess for it that will be awaken when event come back
                    Future<?> future = ex.getFuture();
                    PausedEvent<Future<?>> paused = new PausedEvent<>(topause, future);
                    paused = paused.onSuccess(p.getSuccess())
                            .onFailure(p.getFailure())
                            .onException(p.getException())
                            .setTimeout(ap.getTimeout(), TimeUnit.SECONDS)
                            ;
                    //Create the processor that will process the call back processor
                    @SuppressWarnings({ "rawtypes", "unchecked"})
                    FuturProcessor<?> pauser = new FuturProcessor(future, paused, ap);
                    ex.getEvent().insertProcessor(pauser);

                    //Store the callback informations
                    future.addListener(i -> {
                        inQueue.put(topause);
                        evrepo.cancel(future);
                    });
                    evrepo.pause(paused);
                    status = ProcessingStatus.PAUSED;
                } else {
                    Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".exception").inc();
                    Exception cce = new ClassCastException("A not AsyncProcessor throws a asynchronous operation");
                    Stats.newException(cce);
                    logger.error("A not AsyncProcessor {} throws a asynchronous operation", p);
                    logger.throwing(Level.DEBUG, cce);
                    status = ProcessingStatus.FAILED;
                }
            } catch (ProcessorException.DroppedEventException ex) {
                status = ProcessingStatus.DROPED;
            } catch (IgnoredEventException ex) {
                // A do nothing event
                status = ProcessingStatus.SUCCESS;
            } catch (ProcessorException | UncheckedProcessingException ex) {
                logger.debug("got a processing exception");
                logger.catching(Level.DEBUG, ex);
                e.doMetric(() -> Stats.newError(ex));
                Processor exceptionProcessor = p.getException();
                if (exceptionProcessor != null) {
                    e.insertProcessor(exceptionProcessor);
                    status = ProcessingStatus.SUCCESS;
                } else {
                    status = ProcessingStatus.FAILED;
                }
            } catch (Throwable ex) {
                // We received a fatal exception
                // Can't do nothing but die
                if (Helpers.isFatal(ex)) {
                    throw ex;
                }
                e.doMetric(() -> {
                    Properties.metrics.counter("Pipeline." + e.getCurrentPipeline() + ".exception").inc();
                    Stats.newException(ex);
                });
                String message= ex.getMessage();
                if (message == null) {
                    message = ex.getClass().getCanonicalName();
                } else {
                    message = ex.getClass().getCanonicalName() + ": " + message;
                }
                logger.error("failed to transform event {} with unmanaged error {}", e, message);
                logger.catching(Level.DEBUG, ex);
                status = ProcessingStatus.FAILED;
            }
        }
        return status;
    }

    public void stopProcessing() {
        interrupt();
    }

}
