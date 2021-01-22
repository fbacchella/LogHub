package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import loghub.configuration.Properties;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;

class EventInstance extends Event {

    /**
     * The execution stack contains informations about named pipeline being executed.
     * It's reset when changing top level pipeline
     * @author fa4
     *
     */
    private static final class ExecutionStackElement {
        private static final Logger logger = LogManager.getLogger();
        private final String name;

        private long duration = 0;
        private long startTime = Long.MAX_VALUE;
        private boolean running;

        private ExecutionStackElement(String name) {
            this.name = name;
            restart();
        }

        private void close() {
            if (running) {
                long elapsed = System.nanoTime() - startTime;
                duration += elapsed;
                Stats.pipelineHanding(name, PipelineStat.INFLIGHTDOWN);
            }
            Stats.timerUpdate(name, duration, TimeUnit.NANOSECONDS);
            duration = 0;
            running = false;
            startTime = Long.MAX_VALUE;
        }

        private void pause() {
            running = false;
            Stats.pipelineHanding(name, PipelineStat.INFLIGHTDOWN);
            long elapsed = System.nanoTime() - startTime;
            duration += elapsed;
        }

        private void restart() {
            startTime = System.nanoTime();
            running = true;
            Stats.pipelineHanding(name, PipelineStat.INFLIGHTUP);
        }

        @Override
        public String toString() {
            return name + "(" + Duration.of(duration, ChronoUnit.NANOS) + ")";
        }
    }

    static private final class PreSubpipline extends Processor {

        private final String pipename;

        PreSubpipline(String pipename) {
            this.pipename = pipename;
        }

        @Override
        public boolean configure(Properties properties) {
            return true;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            Optional.ofNullable(event.getRealEvent().executionStack.peek()).ifPresent(ExecutionStackElement::pause);
            ExecutionStackElement ctxt = new ExecutionStackElement(pipename);
            event.getRealEvent().executionStack.add(ctxt);
            ExecutionStackElement.logger.trace("--> {}({})", () -> event.getRealEvent().executionStack, () -> event);
            return true;
        }

        @Override
        public String getName() {
            return "preSubpipline(" + pipename + ")";
        }

        @Override
        public String toString() {
            return "preSubpipline(" + pipename + ")";
        }
    }

    static private final class PostSubpipline extends Processor {

        @Override
        public boolean configure(Properties properties) {
            return true;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            ExecutionStackElement.logger.trace("<-- {}({})", () -> event.getRealEvent().executionStack, () -> event);
            try {
                event.getRealEvent().executionStack.remove().close();
            } catch (NoSuchElementException ex) {
                throw new ProcessorException(event.getRealEvent(), "Empty timer stack, bad state");
            }
            Optional.ofNullable(event.getRealEvent().executionStack.peek()).ifPresent(ExecutionStackElement::restart);
            return true;
        }

        @Override
        public String getName() {
            return "postSubpipline";
        }

        @Override
        public String toString() {
            return "postSubpipline";
        }
    }

    private static final Map<String, PreSubpipline> preSubpiplines = new ConcurrentHashMap<>();
    private static final PostSubpipline postSubpipline = new PostSubpipline();

    private static PreSubpipline getPre(String name) {
        return preSubpiplines.computeIfAbsent(name, PreSubpipline::new);
    }

    private static PostSubpipline getPost(String name) {
        return postSubpipline;
    }

    private static final Logger logger = LogManager.getLogger();

    private transient EventWrapper wevent;
    private transient LinkedList<Processor> processors;

    private String currentPipeline;
    private String nextPipeline;
    private Date timestamp = new Date();
    private final Map<String, Object> metas = new HashMap<>();
    private int stepsCount = 0;
    private boolean test;
    private final ConnectionContext<?> ctx;

    private transient Context timer;

    // The context for exact pipeline timing
    private transient Queue<ExecutionStackElement> executionStack;

    EventInstance(ConnectionContext<?> ctx) {
        this(ctx, false);
    }

    EventInstance(ConnectionContext<?> ctx, boolean test) {
        this.test = test;
        this.ctx = ctx;
        // Initialize the transient objects
        readResolve();
    }

    /**
     * Finish the cloning of the copy
     * Ensure than transient fields store the good values
     * @return
     */
    private void readResolve() {
        if (!test) {
            timer = Stats.eventTimer();
        } else {
            timer = null;
        }
        processors = new LinkedList<>();
        wevent = null;
        executionStack = Collections.asLifoQueue(new ArrayDeque<>());
    }

    public void end() {
        Optional.ofNullable(ctx).ifPresent(ConnectionContext::acknowledge);
        if (! test) {
            timer.close();
            executionStack.forEach(ExecutionStackElement::close);
            Stats.eventEnd(stepsCount);
        } else {
            synchronized(this) {
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
     * It work by doing serialize/deserialize of the event. So a event must
     * only contains serializable object to make it works.
     * <p>
     * It will not duplicate a test event
     * <p>
     * @return a copy of this event, with a different key
     */
    public Event duplicate() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos);) {
            oos.writeObject(this);
            oos.flush();
            bos.flush();
            byte[] byteData = bos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(byteData)) {
                EventInstance forked = (EventInstance) new ObjectInputStream(bais).readObject();
                forked.readResolve();
                return forked;
            }
        } catch (NotSerializableException ex) {
            logger.info("Event copy failed: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return null;
        } catch (ClassNotFoundException | IOException | SecurityException | IllegalArgumentException ex) {
            logger.fatal("Event copy failed: {}", Helpers.resolveThrowableException(ex), ex);
            return null;
        }
    }

    @Override
    public String toString() {
        return "[" + timestamp + "]" + super.toString() + "#" + metas.toString();
    }

    public Processor next() {
        stepsCount++;
        logger.debug("waiting processors {}", processors);
        Processor next = processors.poll();
        if (next == null) {
            wevent = null;
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
        while(append ? i.hasNext() : i.hasPrevious()) {
            Processor p = append ? i.next() : i.previous();
            addProcessor(p, append);
        }
    }

    private void addProcessor(Processor p, boolean append) {
        logger.trace("inject processor {} at {}", () -> p, () -> append ? "end" : "start" );
        if (p instanceof SubPipeline) {
            SubPipeline sp = (SubPipeline) p;
            List<Processor> newProcessors = new ArrayList<>(sp.getPipeline().processors.size() + 2);
            Optional<String> pipeName = Optional.ofNullable(sp.getPipeline().getName());
            pipeName.map(EventInstance::getPre).ifPresent(newProcessors::add);
            newProcessors.addAll(sp.getPipeline().processors);
            pipeName.map(EventInstance::getPost).ifPresent(newProcessors::add);
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
        Optional<String> pipeName = Optional.ofNullable(pipeline.getName());
        pipeName.map(EventInstance::getPre).ifPresent(this::appendProcessor);
        pipeName.ifPresent(s -> currentPipeline = s);
        nextPipeline = pipeline.nextPipeline;
        appendProcessors(pipeline.processors);
        pipeName.map(EventInstance::getPost).ifPresent(this::appendProcessor);
    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Pipeline, java.util.concurrent.BlockingQueue, boolean)
     */
    public boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking) {
        nextPipeline = pipeline.nextPipeline;
        Optional<String>pipeName = Optional.ofNullable(pipeline.getName());
        pipeName.map(EventInstance::getPre).ifPresent(this::appendProcessor);
        pipeName.ifPresent(s -> currentPipeline = s);
        appendProcessors(pipeline.processors);
        pipeName.map(EventInstance::getPost).ifPresent(this::appendProcessor);
        if (blocking) {
            try {
                mainqueue.putBlocking(this);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        } else {
            return mainqueue.offer(this);
        }
    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Event, java.util.concurrent.BlockingQueue)
     */
    public boolean inject(Event ev, PriorityBlockingQueue mainqueue) {
        EventInstance master = ev.getRealEvent();
        currentPipeline = master.currentPipeline;
        nextPipeline = master.nextPipeline;
        appendProcessors(master.processors);
        return mainqueue.offer(this);
    }

    public void finishPipeline() {
        executionStack.forEach(ExecutionStackElement::close);
        executionStack.clear();
        processors.clear();
    }

    public String getCurrentPipeline() {
        return currentPipeline;
    }

    public String getNextPipeline() {
        return nextPipeline;
    }

    @Override
    public boolean process(Processor p) throws ProcessorException {
        if (wevent == null) {
            wevent = new EventWrapper(this);
        }
        wevent.setProcessor(p);
        return p.process(wevent);
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
        if (timestamp != null) {
            this.timestamp = timestamp;
        } else {
            this.timestamp = new Date(0);
        }
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
            String name = Optional.ofNullable(executionStack.peek()).map(c -> c.name).orElse(currentPipeline);
            Stats.pipelineHanding(name, status, ex);
        }
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
    Map<String, Object> getMetas() {
        return metas;
    }

    @Override
    public void mergeMeta(Event event,
                          BiFunction<Object, Object, Object> cumulator) {
        event.getMetas().entrySet().forEach( i-> {
            String key = i.getKey();
            Object newValue = i.getValue();
            Object oldValue = metas.get(key);
            metas.put(key, cumulator.apply(oldValue, newValue));
        });
    }

    @Override
    public Object getMeta(String key) {
        return metas.get(key);
    }

    @Override
    public Object putMeta(String key, Object value) {
        return metas.put(key, value);
    }

    @Override
    public Stream<Entry<String, Object>> getMetaAsStream() {
        return metas.entrySet().stream();
    }

}
