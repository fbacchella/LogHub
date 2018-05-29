package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import loghub.PausingTimer.PausingContext;
import loghub.configuration.Properties;

class EventInstance extends Event {

    static private final class PreSubpipline extends Processor {
        @Override
        public boolean process(Event event) throws ProcessorException {
            PausingContext previouspc = event.getRealEvent().timersStack.peek();
            if (previouspc != null) {
                previouspc.pause();
            }
            String pipename = event.getRealEvent().pipelineNames.remove();
            PausingContext newpc = (PausingContext) Properties.metrics.pausingTimer("Pipeline." + pipename + ".timer").time();
            event.getRealEvent().timersStack.add(newpc);
            return true;
        }

        @Override
        public String getName() {
            return "preSubpipline";
        }

        @Override
        public String toString() {
            return "preSubpipline";
        }
    }

    static private final class PostSubpipline extends Processor {
        @Override
        public boolean process(Event event) throws ProcessorException {
            try {
                event.getRealEvent().timersStack.remove().close();
            } catch (NoSuchElementException e1) {
                throw new ProcessorException(event.getRealEvent(), "Empty timer stack, bad state");
            }
            PausingContext previouspc = event.getRealEvent().timersStack.peek();
            if (previouspc != null) {
                previouspc.restart();
            }
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

    static private final PreSubpipline preSubpipline = new PreSubpipline();
    static private final PostSubpipline postSubpipline = new PostSubpipline();
    
    static boolean configure(Properties props) {
        return postSubpipline.configure(props) && preSubpipline.configure(props);
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

    // The context for exact pipeline timine
    private transient Queue<PausingContext> timersStack;
    private transient Deque<String> pipelineNames;

    EventInstance(ConnectionContext<?> ctx) {
        this(ctx, false);
    }

    EventInstance(ConnectionContext<?> ctx, boolean test) {
        this.test = test;
        this.ctx = ctx;
        if (ctx instanceof IpConnectionContext) {
            IpConnectionContext ipcc = (IpConnectionContext) ctx;
            put("host", ipcc.getRemoteAddress().getAddress());
        }
        // Initialize the transient objects
        readResolve();
    }

    /**
     * Finish the cloning of the copy
     * Ensure than transient fields store the good values
     * @return
     */
    private Object readResolve() {
        if (!test) {
            timer = Properties.metrics.timer("Allevents.timer").time();
            Properties.metrics.counter("Allevents.inflight").inc();
        } else {
            timer = null;
        }
        processors = new LinkedList<>();
        wevent = null;
        timersStack = Collections.asLifoQueue(new ArrayDeque<PausingContext>());
        pipelineNames = new ArrayDeque<>();
        return this;
    }


    public void end() {
        ctx.acknowledge();
        if(! test) {
            timer.close();
            Properties.metrics.counter("Allevents.inflight").dec();
        } else {
            synchronized(this) {
                notify();
            }
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
        ctx.acknowledge();
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            oos.flush();
            oos.close();
            bos.close();
            byte[] byteData = bos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
            return (EventInstance) new ObjectInputStream(bais).readObject();
        } catch (NotSerializableException ex) {
            logger.info("Event copy failed: {}", ex.getMessage());
            logger.catching(Level.DEBUG, ex);
            return null;
        } catch (ClassNotFoundException | IOException ex) {
            logger.fatal("Event copy failed: {}", ex.getMessage());
            logger.catching(Level.FATAL, ex);
            return null;
        }
    }

    @Override
    public String toString() {
        return "[" + timestamp + "]" + super.toString();
    }

    public Processor next() {
        stepsCount++;
        logger.debug("waiting processors {}", processors);
        try {
            return processors.pop();
        } catch (NoSuchElementException e) {
            wevent = null;
            return null;
        }
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
            pipelineNames.add(sp.getPipeline().getName());
            List<Processor> newProcessors = new ArrayList<>(sp.getPipeline().processors.size() + 2);
            newProcessors.add(preSubpipline);
            newProcessors.addAll(sp.getPipeline().processors);
            newProcessors.add(postSubpipline);
            addProcessors(newProcessors, append);
        } else {
            if (append) {
                processors.add(p);
            } else {
                processors.addFirst(p);
            }
        }
    }

    @Override
    public void refill(Pipeline pipeline) {
        currentPipeline = pipeline.getName();
        pipelineNames.add(currentPipeline);
        nextPipeline = pipeline.nextPipeline;
        appendProcessor(preSubpipline);
        appendProcessors(pipeline.processors);
        appendProcessor(postSubpipline);
    }

    /* (non-Javadoc)
     * @see loghub.Event#inject(loghub.Pipeline, java.util.concurrent.BlockingQueue, boolean)
     */
    public boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue, boolean blocking) {
        currentPipeline = pipeline.getName();
        nextPipeline = pipeline.nextPipeline;
        pipelineNames.add(currentPipeline);
        appendProcessor(preSubpipline);
        appendProcessors(pipeline.processors);
        appendProcessor(postSubpipline);
        if (blocking) {
            try {
                mainqueue.put(this);
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
    public boolean inject(Event ev, BlockingQueue<Event> mainqueue) {
        EventInstance master = ev.getRealEvent();
        currentPipeline = master.currentPipeline;
        nextPipeline = master.nextPipeline;
        appendProcessors(master.processors);
        return mainqueue.offer(this);
    }

    public void finishPipeline() {
        timersStack.forEach(PausingContext::close);
        timersStack.clear();
        pipelineNames.clear();
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
    public int stepsCount() {
        return stepsCount;
    }

    @Override
    public boolean isTest() {
        return test;
    }

    @Override
    public void doMetric(Runnable metric) {
        if (! test) {
            metric.run();
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

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        super.putAll(m);
        if (m.containsKey(Event.TIMESTAMPKEY)) {
            if (m.get(Event.TIMESTAMPKEY) instanceof Date) {
                timestamp = (Date) m.get(Event.TIMESTAMPKEY);
            }
            remove(Event.TIMESTAMPKEY);
        }
    }

    @Override
    public ConnectionContext<?> getConnectionContext() {
        return ctx;
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
    public Object getMeta(String key) {
        return metas.get(key);
    }

    @Override
    public Object putMeta(String key, Object value) {
        return metas.put(key, value);
    }

    @Override
    public Object get(Object key) {
        if (Event.CONTEXTKEY.equals(key)) {
            return ctx;
        } else if (Event.TIMESTAMPKEY.equals(key)) {
            return timestamp;
        } else {
            return super.get(key);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        if (Event.CONTEXTKEY.equals(key) || Event.TIMESTAMPKEY.equals(key)) {
            return true;
        } else {
            return super.containsKey(key);
        }
    }

}
