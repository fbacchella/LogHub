package loghub.events;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.SubPipeline;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;

class EventInstance extends Event {

    private static class EventObjectInputStream extends ObjectInputStream {
        private final EventsFactory factory;

        public EventObjectInputStream(InputStream in, EventsFactory factory) throws IOException {
            super(in);
            this.factory = factory;
        }
    }

    private static class EventSerializer implements Externalizable {

        private EventInstance event;

        private EventSerializer(EventInstance event) {
            this.event = event;
        }

        public EventSerializer() {
            // Empty, needed by readExternal
        }

        public void readExternal(ObjectInput input) throws ClassNotFoundException, IOException {
            if (! (input instanceof EventObjectInputStream)) {
                throw new IllegalStateException();
            }
            EventObjectInputStream eois = (EventObjectInputStream) input;
            ConnectionContext ctx;
            int contextIndicator = eois.readInt();
            switch (contextIndicator) {
            case 0:
                ctx = ConnectionContext.EMPTY;
                break;
            default:
                ctx = (ConnectionContext) eois.readObject();
                break;
            }
            event = (EventInstance) eois.factory.newEvent(ctx);
            event.timestamp = new Date(eois.readLong());
            event.currentPipeline = eois.readUTF();
            boolean nextPipeline = eois.readBoolean();
            if(nextPipeline) {
                event.nextPipeline = eois.readUTF();
            }
            event.stepsCount = eois.readInt();
            event.test = eois.readBoolean();
            readMap(event.getMetas(), eois);
            readMap(event, eois);
        }

        public void writeExternal(ObjectOutput output) throws IOException {
            ConnectionContext<?> ctx = event.getConnectionContext();
            if (ctx == ConnectionContext.EMPTY) {
                output.writeInt(0);
            } else {
                output.writeInt(1);
                output.writeObject(ctx);
            }
            output.writeLong(event.timestamp.getTime());
            output.writeUTF(event.currentPipeline);
            boolean nextPipeline = event.nextPipeline != null;
            output.writeBoolean(nextPipeline);
            if (nextPipeline) {
                output.writeUTF(event.nextPipeline);
            }
            output.writeInt(event.stepsCount);
            output.writeBoolean(event.test);
            writeMap(event.metas, output);
            writeMap(event, output);
        }

        private void writeMap(Map<String, Object> map, ObjectOutput output) throws IOException {
            output.writeInt(map.size());
            try {
                map.entrySet().forEach(e -> {
                    try {
                        output.writeUTF(e.getKey());
                        output.writeObject(e.getValue());
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        private void readMap(Map<String, Object> map, ObjectInputStream aInputStream)
                throws IOException, ClassNotFoundException {
            int size = aInputStream.readInt();
            for (int i = 0; i < size ; i++) {
                String key = aInputStream.readUTF();
                Object value = aInputStream.readObject();
                map.put(key, value);
            }
        }
    }

    private static final Logger logger = LogManager.getLogger();

    private EventWrapper wevent;
    private LinkedList<Processor> processors;

    private String currentPipeline;
    private String nextPipeline;
    private Date timestamp = new Date();
    private Map<String, Object> metas = new HashMap<>();
    private int stepsCount = 0;
    private boolean test;
    private final ConnectionContext<?> ctx;
    private final EventsFactory factory;

    private Context timer;

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
            oos.writeObject(new EventSerializer(this));
            oos.flush();
            bos.flush();
            byte[] byteData = bos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(byteData) ; ObjectInputStream ois = new EventObjectInputStream(bais, factory)) {
                return ((EventSerializer) ois.readObject()).event;
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
            pipeName.map(factory::getPre).ifPresent(newProcessors::add);
            newProcessors.addAll(sp.getPipeline().processors);
            pipeName.map(s -> PostSubpipline.INSTANCE).ifPresent(this::appendProcessor);
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
        Optional<String> pipeName = Optional.ofNullable(pipeline.getName());
        pipeName.map(factory::getPre).ifPresent(this::appendProcessor);
        pipeName.ifPresent(s -> currentPipeline = s);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubpipline.INSTANCE).ifPresent(this::appendProcessor);

    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Pipeline, java.util.concurrent.BlockingQueue, boolean)
     */
    public boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking) {
        nextPipeline = pipeline.nextPipeline;
        Optional<String>pipeName = Optional.ofNullable(pipeline.getName());
        pipeName.map(factory::getPre).ifPresent(this::appendProcessor);
        pipeName.ifPresent(s -> currentPipeline = s);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubpipline.INSTANCE).ifPresent(this::appendProcessor);
        return mainqueue.inject(this, blocking);
    }

    public void reinject(Pipeline pipeline, PriorityBlockingQueue mainqueue) {
        nextPipeline = pipeline.nextPipeline;
        Optional<String>pipeName = Optional.ofNullable(pipeline.getName());
        pipeName.map(factory::getPre).ifPresent(this::appendProcessor);
        pipeName.ifPresent(s -> currentPipeline = s);
        appendProcessors(pipeline.processors);
        pipeName.map(s -> PostSubpipline.INSTANCE).ifPresent(this::appendProcessor);
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
    public Map<String, Object> getMetas() {
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
