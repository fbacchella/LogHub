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
import loghub.events.Event;
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
        ERROR,
        DISCARD,
    }

    private static final Logger logger = LogManager.getLogger();
    private static final AtomicInteger id = new AtomicInteger();

    private final BlockingQueue<Event> inQueue;
    private final Map<String, BlockingQueue<Event>> outQueues;
    private final Map<String, Pipeline> namedPipelines;
    private final int maxSteps;
    private final EventsRepository<Future<?>> evrepo;

    // Used to handle events async processing
    private final BlockingQueue<Event> blockedAsync = new LinkedBlockingQueue<>();
    private Event lastblockedAsync = null;

    public EventsProcessor(PriorityBlockingQueue inQueue, Map<String, BlockingQueue<Event>> outQueues, Map<String, Pipeline> namedPipelines, int maxSteps, EventsRepository<Future<?>> evrepo) {
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

            Event event;
            try {
                event = inQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            { // Needed because eventtemp must be final
                final Event eventtemp  = event;
                logger.trace("received {}", () -> eventtemp);
            }
            Context tctxt = Stats.startProcessingEvent();
            Processor processor = event.next();
            while (processor != null) {
                event.getPipelineLogger().trace("processing with {}", processor);
                if (processor instanceof WrapEvent) {
                    try {
                        event = event.wrap(processor.getPathArray());
                    } catch (IgnoredEventException e) {
                        // Wrapped failed, trying to descent in a non path
                        event.getPipelineLogger().debug("Not usable path while wrapping, ignored event {}", event);
                        int depth = 1;
                        // Remove all processing until the matching unwrap is done
                        while (depth > 0) {
                            processor = event.next();
                            if (processor instanceof WrapEvent) {
                                depth++;
                            } else if (processor instanceof UnwrapEvent) {
                                depth--;
                            }
                        }
                    }
                } else if (processor instanceof UnwrapEvent) {
                    event = event.unwrap();
                } else {
                    ProcessingStatus processingstatus = process(event, processor);
                    if (processingstatus != ProcessingStatus.CONTINUE) {
                        // Processing status was not null, so the event will not be processed any more
                        // But it's needed to check why.
                        switch (processingstatus) {
                        case DROPED: {
                            // It was a drop action
                            event.getPipelineLogger().debug("Dropped event {}", event);
                            event.drop();
                            break;
                        }
                        case DISCARD: {
                            event.getPipelineLogger().debug("Discarded event {}", event);
                            event.end();
                            break;
                        }
                        case ERROR: {
                            // Processing failed critically (with an exception) and no recovery was attempted
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
                // Tests event are not forwarded to next pipeline
                while (processor == null && event.getNextPipeline() != null && ! event.isTest()) {
                    event.getPipelineLogger().trace("next pipeline is {}", event.getNextPipeline());
                    // Send to another pipeline, loop in the main processing queue
                    Pipeline next = namedPipelines.get(event.getNextPipeline());
                    if (next == null) {
                        event.getPipelineLogger().error("Failed to forward to pipeline {}, not found", event.getNextPipeline());
                        event.drop();
                        break;
                    } else {
                        event.finishPipeline();
                        event.refill(next);
                        processor = event.next();
                    }
                }
            }
            if (event != null) {
                logger.trace("event is now {}", event);
                // Processing of the event is finished without being abandoned, what to do next with it ?
                // Detect if it will send to another pipeline, or just wait for a sender to take it
                if (event.isTest()) {
                    // A test event, it will not be sent an output queue
                    // Checked after pipeline forwarding, but before output sending
                    TestEventProcessing.log(event);
                    try {
                        outQueues.get(event.getCurrentPipeline()).put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    event.end();
                } else if (event.getCurrentPipeline() != null && outQueues.containsKey(event.getCurrentPipeline())) {
                    // Put in the output queue, where the wanting output will come to take it
                    try {
                        outQueues.get(event.getCurrentPipeline()).put(event);
                    } catch (InterruptedException e) {
                        event.doMetric(PipelineStat.EXCEPTION, e);
                        event.end();
                        Thread.currentThread().interrupt();
                    }
                } else if (event.getCurrentPipeline() != null && ! outQueues.containsKey(event.getCurrentPipeline())) {
                    event.doMetric(PipelineStat.EXCEPTION, new IllegalArgumentException("No sender consuming pipeline " + event.getCurrentPipeline()));
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
        ProcessingStatus status;
        if (p instanceof Forker) {
            ((Forker) p).fork(e);
            status = ProcessingStatus.CONTINUE;
        } else if (p instanceof Forwarder) {
            ((Forwarder) p).forward(e);
            status = ProcessingStatus.CONTINUE;
        } else if (p instanceof Drop) {
            e.doMetric(Stats.PipelineStat.DROP);
            status = ProcessingStatus.DROPED;
        } else if (e.processingDone() > maxSteps) {
            e.getPipelineLogger().warn("Too much steps for an event in pipeline. Done {} steps, still {} left, looping event: {}", e::processingDone, e::processingLeft, () -> e);
            e.doMetric(Stats.PipelineStat.LOOPOVERFLOW);
            status = ProcessingStatus.ERROR;
        } else {
            try {
                if (p.isprocessNeeded(e)) {
                    boolean success = e.process(p);
                    // After processing, check the failures and success processors
                    Processor nextProcessor = success ? p.getSuccess() : p.getFailure();
                    if (nextProcessor != null) {
                        e.insertProcessor(nextProcessor);
                    }
                }
                status = ProcessingStatus.CONTINUE;
            } catch (AsyncProcessor.PausedEventException ex) {
                // First check if the process will be able to manage the call back
                if (p instanceof AsyncProcessor && ex.getFuture() != null) {
                    @SuppressWarnings("unchecked")
                    AsyncProcessor<?, Future<?>> ap = (AsyncProcessor<?, Future<?>>) p;
                    // The event to pause might be a transformation of the original event.
                    // A paused event was catch, create a custom FuturProcess for it that will be awakened when event come back
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
                        e.getPipelineLogger().error("Interrupted");
                        e.getPipelineLogger().catching(Level.DEBUG, iex);
                        future.cancel(true);
                        return ProcessingStatus.ERROR;
                    }
                    // Compilation fails if builder is used directly
                    PausedEvent.Builder<Future<?>> builder = PausedEvent.builder(e, future);
                    PausedEvent<Future<?>> paused = builder
                                    .onSuccess(p.getSuccess())
                                    .onFailure(p.getFailure())
                                    .onException(p.getException())
                                    .expiration(ap.getTimeout(), TimeUnit.SECONDS)
                                    .onTimeout(ap.getTimeoutHandler())
                                    .build();
                    if (evrepo.pause(paused)) {
                        // Create the processor that will process the call back processor
                        @SuppressWarnings({ "rawtypes", "unchecked"})
                        FutureProcessor<?, ? extends Future<?>> pauser = new FutureProcessor(future, paused, ap);
                        e.insertProcessor(pauser);
                        // Store the callback information
                        future.addListener(i -> {
                            ap.getLimiter().ifPresent(Semaphore::release);
                            evrepo.cancel(future);
                            // the listener must not call blocking call.
                            // So if the bounded queue block, use a non-blocking queue dedicated
                            // to postpone processing of the offer.
                            if (! inQueue.offer(e)) {
                                blockedAsync.put(e);
                            }
                        });
                        status = ProcessingStatus.PAUSED;
                    } else {
                        status = ProcessingStatus.DROPED;
                        e.doMetric(Stats.PipelineStat.DROP);
                    }
                } else if (ex.getFuture() == null) {
                    // No future, internal handling of the pause
                    status = ProcessingStatus.PAUSED;
                } else {
                    e.doMetric(Stats.PipelineStat.EXCEPTION, ex);
                    e.getPipelineLogger().error("Paused event from a non async processor {}, can't handle", p.getName());
                    e.getPipelineLogger().catching(Level.DEBUG, ex);
                    status = ProcessingStatus.ERROR;
                }
            } catch (DiscardedEventException ex) {
                status = ProcessingStatus.DISCARD;
                e.doMetric(PipelineStat.DISCARD);
            } catch (IgnoredEventException ex) {
                // A "do nothing" process
                status = ProcessingStatus.CONTINUE;
            } catch (ProcessorException | UncheckedProcessorException ex) {
                ProcessorException rex = ex instanceof ProcessorException ? (ProcessorException) ex : ((UncheckedProcessorException) ex).getProcessorException();
                Processor exceptionProcessor = p.getException();
                e.getPipelineLogger().atLevel(exceptionProcessor == null ? Level.WARN : Level.DEBUG)
                                     .withThrowable(rex)
                                     .log("Got the processing exception {} for event {}", rex::getMessage, () -> e);
                if (exceptionProcessor != null) {
                    e.pushException(rex);
                    e.insertProcessor(exceptionProcessor);
                    status = ProcessingStatus.CONTINUE;
                } else {
                    e.doMetric(Stats.PipelineStat.FAILURE, rex);
                    status = ProcessingStatus.ERROR;
                }
            } catch (Throwable ex) {
                // We received a fatal exception
                // Can't do anything but die
                if (Helpers.isFatal(ex)) {
                    logger.fatal("Caught a critical exception", ex);
                    ShutdownTask.fatalException(ex);
                } else {
                    e.doMetric(Stats.PipelineStat.EXCEPTION, ex);
                    Logger plogger =  e.getPipelineLogger();
                    plogger.atError()
                           .withThrowable(plogger.isDebugEnabled() ? ex : null)
                           .log("Failed to transform event {} with unmanaged error {}", e, Helpers.resolveThrowableException(ex));
                }
                status = ProcessingStatus.ERROR;
            }
        }
        return status;
    }

    public void stopProcessing() {
        interrupt();
    }

}
