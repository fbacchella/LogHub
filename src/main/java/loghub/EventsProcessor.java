package loghub;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import io.netty.util.concurrent.Future;
import loghub.configuration.TestEventProcessing;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;
import loghub.processors.Drop;
import loghub.processors.Forker;
import loghub.processors.Forwarder;
import loghub.processors.FutureProcessor;
import loghub.processors.UnwrapEvent;
import loghub.processors.WrapEvent;

public class EventsProcessor extends Thread {

    enum ProcessingStatus {
        CONTINUE,
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
    
    // Used to handle events async processing
    private final BlockingQueue<Event> blockedAsync = new LinkedBlockingQueue<>();
    private Event lastblockedAsync = null;

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
        while (! isInterrupted()) {
            Event lasttryblockedAsync;
            // Used to detect if a an async processor was blocked
            // blockedAsync is the queue of such events.
            // lasttryblockedAsync is kind of flag that is used to detect that blockedAsync.poll() or inQueue.offer() succeeded.
            do {
                lasttryblockedAsync = lastblockedAsync;
                if (lastblockedAsync != null) {
                    if (inQueue.offer(lastblockedAsync)) {
                        lastblockedAsync = blockedAsync.poll();
                    }
                } else {
                    lastblockedAsync = blockedAsync.poll();
                }
            } while (lasttryblockedAsync != lastblockedAsync);

            Event event = null;
            try {
                event = inQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            { // Needed because eventtemp must be final
                final Event eventtemp  = event;
                logger.trace("received {} in {}", () -> eventtemp, () -> eventtemp.getCurrentPipeline());
            }
            Context tctxt = Stats.startProcessingEvent();
            Processor processor = event.next();
            while (processor != null) {
                logger.trace("processing with {}", processor);
                if (processor instanceof WrapEvent) {
                    event = new EventWrapper(event, processor.getPathArray());
                } else if (processor instanceof UnwrapEvent) {
                    event = event.unwrap();
                } else {
                    ProcessingStatus processingstatus = process(event, processor);
                    if (processingstatus != ProcessingStatus.CONTINUE) {
                        // Processing status was non null, so the event will not be processed any more
                        // But it's needed to check why.
                        switch (processingstatus) {
                        case DROPED: {
                            //It was a drop action
                            logger.debug("Dropped event {}", event);
                            event.drop();
                            break;
                        }
                        case FAILED: {
                            //Processing failed critically (with an exception) and no recovery was attempted
                            logger.debug("Failed event {}", event);
                            event.end();
                            break;
                        }
                        default:
                            // Non fatal processing interruption
                            break;
                        }
                        event = null;
                        break;
                    }
                }
                processor = event.next();
                // If next processor is null, refill the event
                while (processor == null && event.getNextPipeline() != null) {
                    logger.trace("next processor is {}", processor);
                    // Send to another pipeline, loop in the main processing queue
                    Pipeline next = namedPipelines.get(event.getNextPipeline());
                    if (next == null) {
                        logger.error("Failed to forward to pipeline {} from {}, not found", event.getNextPipeline(), event.getCurrentPipeline());
                        event.drop();
                        break;
                    } else {
                        event.refill(next);
                        processor = event.next();
                    }
                }
            }
            logger.trace("event is now {}", event);
            // Processing of the event is finished, what to do next with it ?
            // Detect if will send to another pipeline, or just wait for a sender to take it
            if (event != null) {
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
                        event.doMetric(PipelineStat.EXCEPTION, e);
                        event.end();
                        Thread.currentThread().interrupt();
                    }
                } else if (event.getCurrentPipeline() != null && ! outQueues.containsKey(event.getCurrentPipeline())){
                    event.doMetric(PipelineStat.EXCEPTION, new IllegalArgumentException("No sender consumming pipeline " + event.getCurrentPipeline()));
                    logger.debug("No sender using pipeline {} for event {}", event.getCurrentPipeline(), event);
                    event.end();
                } else {
                    event.doMetric(PipelineStat.EXCEPTION, new IllegalStateException("Invalid end state for event, no pipeline"));
                    logger.debug("Invalid end state for event {}", event);
                    event.end();
                }
            }
            Stats.endProcessingEvent(tctxt);
        }
    }

    ProcessingStatus process(Event e, Processor p) {
        ProcessingStatus status = null;
        if (p instanceof Forker) {
            if (((Forker) p).fork(e)) {
                status = ProcessingStatus.CONTINUE;
            } else {
                status = ProcessingStatus.FAILED;
            }
        } else if (p instanceof Forwarder) {
            ((Forwarder) p).forward(e);
            status = ProcessingStatus.CONTINUE;
        } else if (p instanceof Drop) {
            status = ProcessingStatus.DROPED;
            e.doMetric(Stats.PipelineStat.DROP);
        } else if (e.processingDone() > maxSteps) {
            logger.error("Too much steps for an event in pipeline. Done {} steps, still {} left, throwing away", () -> e.processingDone(), () -> e.processingLeft());
            logger.debug("Thrown event: {}", e);
            e.doMetric(Stats.PipelineStat.LOOPOVERFLOW);
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
                status = ProcessingStatus.CONTINUE;
            } catch (ProcessorException.PausedEventException ex) {
                // First check if the process will be able to manage the call back
                if (p instanceof AsyncProcessor && ex.getFuture() != null) {
                    @SuppressWarnings("unchecked")
                    AsyncProcessor<?, Future<?>> ap = (AsyncProcessor<?, Future<?>>) p;
                    // The event to pause might be a transformation of the original event.
                    Event topause = ex.getEvent();
                    // A paused event was catch, create a custom FuturProcess for it that will be awaken when event come back
                    Future<?> future = ex.getFuture();
                    // Wait if too much asynchronous event are waiting
                    try {
                        ap.getLimiter().ifPresent(t -> {
                            try {
                                t.acquire();
                            } catch (InterruptedException iex) {
                                throw new UndeclaredThrowableException(iex);
                            }
                        });
                    } catch (UndeclaredThrowableException uex) {
                        Thread.currentThread().interrupt();
                        InterruptedException iex = (InterruptedException) uex.getCause();
                        e.doMetric(Stats.PipelineStat.FAILURE, iex);
                        logger.error("Interrupted");
                        logger.catching(Level.DEBUG, iex);
                        future.cancel(true);
                        status = ProcessingStatus.FAILED;
                    }
                    // Compilation fails if builder is used directly
                    PausedEvent.Builder<Future<?>> builder = PausedEvent.builder(topause, future);
                    PausedEvent<Future<?>> paused = builder
                                    .onSuccess(p.getSuccess())
                                    .onFailure(p.getFailure())
                                    .onException(p.getException())
                                    .expiration(ap.getTimeout(), TimeUnit.SECONDS)
                                    .onTimeout(ap.getTimeoutHandler())
                                    .build();
                    // Create the processor that will process the call back processor
                    @SuppressWarnings({ "rawtypes", "unchecked"})
                    FutureProcessor<?, ? extends Future<?>> pauser = new FutureProcessor(future, paused, ap);
                    topause.insertProcessor(pauser);
                    //Store the callback informations
                    future.addListener(i -> {
                        ap.getLimiter().ifPresent(Semaphore::release);
                        evrepo.cancel(future);
                        // the listener must not call blocking call.
                        // So if the bounded queue block, use a non blocking queue dedicated
                        // to postpone processing of the offer.
                        if (! inQueue.offer(topause)) {
                            blockedAsync.put(topause);
                        }
                    });
                    evrepo.pause(paused);
                    status = ProcessingStatus.PAUSED;
                } else if (ex.getFuture() == null) {
                    // No future, internal handling of the pause
                    status = ProcessingStatus.PAUSED;
                } else {
                    e.doMetric(Stats.PipelineStat.EXCEPTION, ex);
                    logger.error("Paused event from a non async processor {}, can't handle", p.getName());
                    logger.catching(Level.DEBUG, ex);
                    status = ProcessingStatus.FAILED;
                }
            } catch (ProcessorException.DroppedEventException ex) {
                status = ProcessingStatus.DROPED;
                e.doMetric(Stats.PipelineStat.DROP);
            } catch (IgnoredEventException ex) {
                // A do nothing process
                status = ProcessingStatus.CONTINUE;
            } catch (ProcessorException | UncheckedProcessorException ex) {
                logger.debug("got a processing exception: {}", ex);
                logger.catching(Level.DEBUG, ex);
                Processor exceptionProcessor = p.getException();
                if (exceptionProcessor != null) {
                    e.insertProcessor(exceptionProcessor);
                    status = ProcessingStatus.CONTINUE;
                } else {
                    e.doMetric(Stats.PipelineStat.FAILURE, ex);
                    status = ProcessingStatus.FAILED;
                }
            } catch (Throwable ex) {
                // We received a fatal exception
                // Can't do nothing but die
                if (Helpers.isFatal(ex)) {
                    throw ex;
                }
                e.doMetric(Stats.PipelineStat.EXCEPTION, ex);
                logger.error("failed to transform event {} with unmanaged error {}", e, Helpers.resolveThrowableException(ex));
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
