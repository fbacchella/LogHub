package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import loghub.configuration.Properties;

class EventInstance extends Event {

    private final static Logger logger = LogManager.getLogger();

    private transient EventWrapper wevent;
    private transient LinkedList<Processor> processors = new LinkedList<>();
    private String currentPipeline;
    private String nextPipeline;
    private Date timestamp = new Date();
    private transient Context timer;
    private int stepsCount = 0;
    private boolean test;
    private final ConnectionContext ctx;

    EventInstance(ConnectionContext ctx) {
        this(ctx, false);
    }

    EventInstance(ConnectionContext ctx, boolean test) {
        this.test = test;
        this.ctx = ctx;
        if (ctx instanceof IpConnectionContext) {
            IpConnectionContext ipcc = (IpConnectionContext) ctx;
            put("host", ipcc.getRemoteAddress().getAddress());
        }
        if (! test) {
            Properties.metrics.counter("Allevents.inflight").inc();
            timer = Properties.metrics.timer("Allevents.timer").time();
        } else {
            timer = null;
        }
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
            EventInstance newEvent = (EventInstance) new ObjectInputStream(bais).readObject();
            newEvent.processors = new LinkedList<>();
            newEvent.wevent = null;
            return newEvent;
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
            Processor p = processors.pop();
            return p;
        } catch (NoSuchElementException e) {
            wevent = null;
            return null;
        }
    }

    public void insertProcessor(Processor p) {
        inject(p, false);
    }

    public void appendProcessor(Processor p) {
        inject(p, true);
    }

    public void insertProcessors(List<Processor> p) {
        inject(p, false);
    }

    public void appendProcessors(List<Processor> p) {
        inject(p, true);
    }

    /**
     * This method inject a new event in a pipeline as
     * a top processing pipeline. Not to be used for sub-processing pipeline
     * @param event
     */
    public boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue) {
        currentPipeline = pipeline.getName();
        nextPipeline = pipeline.nextPipeline;
        appendProcessors(pipeline.processors);
        return mainqueue.offer(this);
    }

    public boolean inject(Event ev, BlockingQueue<Event> mainqueue) {
        EventInstance master = ev.getRealEvent();
        currentPipeline = master.currentPipeline;
        nextPipeline = master.nextPipeline;
        appendProcessors(master.processors);
        return mainqueue.offer(this);
    }

    public void finishPipeline() {
        processors.clear();
    }

    private void inject(List<Processor> newProcessors, boolean append) {
        ListIterator<Processor> i = newProcessors.listIterator(append ? 0 : newProcessors.size());
        while(append ? i.hasNext() : i.hasPrevious()) {
            Processor p = append ? i.next() : i.previous();
            inject(p, append);
        }
    }

    private void inject(Processor p, boolean append) {
        logger.trace("inject processor {} at {}", () -> p, () -> append ? "end" : "start" );
        if (p instanceof SubPipeline) {
            SubPipeline sp = (SubPipeline) p;
            inject(sp.getPipeline().processors, append);
        } else {
            if (append) {
                processors.add(p);
            } else {
                processors.addFirst(p);
            }
        }
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
        this.timestamp = timestamp;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (!test) {
            timer = Properties.metrics.timer("Allevents.timer").time();
            Properties.metrics.counter("Allevents.inflight").inc();
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
    public ConnectionContext getConnectionContext() {
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

}
