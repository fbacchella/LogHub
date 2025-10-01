package loghub.events;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.NullOrMissingValue;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.SubPipeline;
import loghub.VariablePath;
import loghub.cloners.DeepCloner;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;
import lombok.Getter;

class EventInstance extends Event {

    static {
        DeepCloner.register(EventInstance.class, o -> ((EventInstance)o).clone());
    }

    private static final Logger logger = LogManager.getLogger();

    private final LinkedList<Processor> processors;

    @Getter
    private String currentPipeline;
    @Getter
    private String nextPipeline;
    private Date timestamp = new Date();
    private final Map<String, Object> metas = new HashMap<>();
    private int stepsCount = 0;
    private boolean test;
    private final ConnectionContext<?> ctx;
    private final EventsFactory factory;
    private Context timer;
    private Logger pipeLineLogger;
    private final EventFinalizer ref;

    // The context for exact pipeline timing
    Queue<ExecutionStackElement> executionStack;

    EventInstance(ConnectionContext<?> ctx, EventsFactory factory) {
        this(ctx, false, factory);
    }

    EventInstance(ConnectionContext<?> ctx, boolean test, EventsFactory factory) {
        this.test = test;
        this.ctx = ctx;
        this.factory = factory;
        if (!test) {
            timer = Stats.eventTimer();
        }
        processors = new LinkedList<>();
        executionStack = Collections.asLifoQueue(new ArrayDeque<>());
        ref = new EventFinalizer(this, factory.referenceQueue, timer);
    }

    public void end() {
        ref.clear();
        Optional.ofNullable(ctx).ifPresent(ConnectionContext::acknowledge);
        if (! test) {
            EventsFactory.finishEvent(false, timer);
            String pipeName = Optional.of(executionStack)
                                      .map(Queue::peek)
                                      .map(e -> e.pipe)
                                      .map(Pipeline::getName)
                                      .orElse(null);
            Stats.eventEnd(pipeName, stepsCount);
        } else {
            synchronized (this) {
                notify();
            }
        }
    }

    @Override
    public void drop() {
        end();
        if (test) {
            clear();
            put("_processing_dropped", Boolean.TRUE);
        }
    }

    /**
     * Return a deep copy of the event.
     * <p>
     * It works by doing serialize/deserialize of the event. So an event must
     * only contain serializable object to make it works.
     * <p>
     * It will not duplicate a test event
     * <p>
     * @return a copy of this event, with a different key
     */
    public Event duplicate() throws ProcessorException {
        try {
            return DeepCloner.clone(this);
        } catch (RuntimeException ex) {
            throw new ProcessorException(this, "Unable to serialise event : " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    @Override
    public String toString() {
        return "[" + timestamp + "]" + super.toString() + "#" + metas;
    }

    public Processor next() {
        stepsCount++;
        logger.debug("waiting processors {}", processors);
        Processor next = processors.poll();
        if (currentPipeline == null
              || pipeLineLogger == null
              && next instanceof PreSubPipline) {
            // It's starting processing, so current pipeline information not yet initialized
            Pipeline ppl = ((PreSubPipline) next).getPipe();
            if (ppl.getName() != null) {
                pipeLineLogger = ppl.getLogger();
                currentPipeline = ppl.getName();
            }
        }
        return next;
    }

    public void insertProcessor(Processor p) {
        addProcessor(p, false);
    }

    public void appendProcessor(Processor p) {
        addProcessor(p, true);
    }

    public void insertProcessors(List<Processor> p) {
        addProcessors(p, false);
    }

    public void appendProcessors(List<Processor> p) {
        addProcessors(p, true);
    }

    private void addProcessors(List<Processor> newProcessors, boolean append) {
        ListIterator<Processor> i = newProcessors.listIterator(append ? 0 : newProcessors.size());
        while (append ? i.hasNext() : i.hasPrevious()) {
            Processor p = append ? i.next() : i.previous();
            addProcessor(p, append);
        }
    }

    private void addProcessor(Processor p, boolean append) {
        logger.trace("inject processor {} at {}", () -> p, () -> append ? "end" : "start");
        if (p instanceof SubPipeline) {
            SubPipeline sp = (SubPipeline) p;
            Pipeline pipe = sp.getPipeline();
            List<Processor> newProcessors = new ArrayList<>(sp.getPipeline().processors.size() + 2);
            Optional<Pipeline> namedPipeline = Optional.ofNullable(pipe).filter(ppl -> ppl.getName() != null);
            namedPipeline.map(Pipeline::getPreSubPipline).ifPresent(newProcessors::add);
            newProcessors.addAll(sp.getPipeline().processors);
            namedPipeline.map(s -> PostSubPipline.INSTANCE).ifPresent(newProcessors::add);
            addProcessors(newProcessors, append);
        } else {
            if (append) {
                processors.addLast(p);
            } else {
                processors.addFirst(p);
            }
        }
    }

    @Override
    public void refill(Pipeline pipeline) {
        nextPipeline = pipeline.nextPipeline;
        Optional<Pipeline> pipeName = Optional.of(pipeline).filter(ppl -> ppl.getName() != null);
        pipeName.map(Pipeline::getPreSubPipline).ifPresent(this::appendProcessor);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubPipline.INSTANCE).ifPresent(this::appendProcessor);
    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Pipeline, java.util.concurrent.BlockingQueue, boolean)
     */
    public boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking) {
        nextPipeline = pipeline.nextPipeline;
        Optional<Pipeline> pipeName = Optional.of(pipeline).filter(ppl -> ppl.getName() != null);
        pipeName.map(Pipeline::getPreSubPipline).ifPresent(this::appendProcessor);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubPipline.INSTANCE).ifPresent(this::appendProcessor);
        return mainqueue.inject(this, blocking);
    }

    public void reinject(Pipeline pipeline, PriorityBlockingQueue mainqueue) {
        nextPipeline = pipeline.nextPipeline;
        Optional<Pipeline> pipeName = Optional.of(pipeline).filter(ppl -> ppl.getName() != null);
        pipeName.map(Pipeline::getPreSubPipline).ifPresent(this::appendProcessor);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubPipline.INSTANCE).ifPresent(this::appendProcessor);
        mainqueue.asyncInject(this);
    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Event, java.util.concurrent.BlockingQueue)
     */
    public void reinject(Event ev, PriorityBlockingQueue mainqueue) {
        EventInstance master = ev.getRealEvent();
        currentPipeline = master.currentPipeline;
        nextPipeline = master.nextPipeline;
        appendProcessors(master.processors);
        mainqueue.asyncInject(this);
    }

    @Override
    public void finishPipeline() {
        executionStack.forEach(ExecutionStackElement::close);
        executionStack.clear();
        processors.clear();
        currentPipeline = null;
        pipeLineLogger = null;
    }

    @Override
    public boolean process(Processor p) throws ProcessorException {
        if (p.getPathArray() == VariablePath.EMPTY) {
            return p.process(this);
        } else {
            EventWrapper wevent = new EventWrapper(this, p.getPathArray());
            return p.process(wevent);
        }
    }

    /**
     * @return the timestamp
     */
    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    @Override
    public void setTimestamp(Date timestamp) {
        this.timestamp = Objects.requireNonNullElseGet(timestamp, () -> new Date(0));
    }

    @Override
    public boolean containsData() {
        return true;
    }

    @Override
    public int processingDone() {
        return stepsCount;
    }

    @Override
    public int processingLeft() {
        return processors.size();
    }

    @Override
    public boolean isTest() {
        return test;
    }

    @Override
    public void doMetric(PipelineStat status, Throwable ex) {
        if (! test) {
            Stats.pipelineHanding(this, status, ex);
        }
    }

    @Override
    public String getRunningPipeline() {
        return Optional.ofNullable(executionStack.peek()).map(c -> c.pipe.getName()).orElse(currentPipeline);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ConnectionContext<T> getConnectionContext() {
        return (ConnectionContext<T>) ctx;
    }

    @Override
    protected EventInstance getRealEvent() {
        return this;
    }

    @Override
    public Event unwrap() {
        return this;
    }

    @Override
    public Map<String, Object> getMetas() {
        return metas;
    }

    @Override
    public void mergeMeta(Event event,
                          BiFunction<Object, Object, Object> cumulator) {
        event.getMetas().forEach((key, newValue) -> {
            Object oldValue = metas.get(key);
            metas.put(key, cumulator.apply(oldValue, newValue));
        });
    }

    @Override
    public Object getMeta(String key) {
        if (metas.containsKey(key)) {
            return metas.get(key);
        } else {
            return NullOrMissingValue.MISSING;
        }
    }

    @Override
    public Object putMeta(String key, Object value) {
        return metas.put(key, value);
    }

    @Override
    public Stream<Entry<String, Object>> getMetaAsStream() {
        return metas.entrySet().stream();
    }

    void refreshLogger() {
        executionStack.stream()
                      .map(ExecutionStackElement::getLogger)
                      .filter(Objects::nonNull)
                      .findFirst()
                      .ifPresent(ppl -> pipeLineLogger = ppl);
    }

    public Logger getPipelineLogger() {
        return pipeLineLogger != null ? pipeLineLogger : LogManager.getLogger();
    }

    @Override
    public Object clone() {
        EventInstance newEvent = (EventInstance) factory.newEvent(ctx);
        newEvent.setTimestamp(timestamp);
        newEvent.currentPipeline = currentPipeline;
        newEvent.nextPipeline = nextPipeline;
        newEvent.stepsCount = stepsCount;
        newEvent.test = test;
        newEvent.metas.clear();
        metas.forEach((k, v) -> newEvent.metas.put(k, DeepCloner.clone(v)));
        newEvent.clear();
        forEach((k, v) -> newEvent.put(k, DeepCloner.clone(v)));
        return newEvent;
    }

}
