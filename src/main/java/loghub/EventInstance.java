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
    private transient Context timer = Properties.metrics.timer("Allevents.timer").time();
    private int stepsCount = 0;

    EventInstance() {
        Properties.metrics.counter("Allevents.inflight").inc();
    }

    public void end() {
        timer.close();
        Properties.metrics.counter("Allevents.inflight").dec();
    }

    /**
     * Return a deep copy of the event.
     * 
     * It work by doing serialize/deserialize of the event. So a event must
     * only contains serializable object to make it works.
     * 
     * @return a copy of this event, with a different key
     */
    public Event duplicate() {
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

    @Override
    public ProcessorException buildException(String message) {
        return new ProcessorException(this, message);
    }

    @Override
    public ProcessorException buildException(String message, Exception root) {
        return new ProcessorException(this, message, root);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        timer = Properties.metrics.timer("Allevents.timer").time();
        Properties.metrics.counter("Allevents.inflight").inc();
    }

    @Override
    public int stepsCount() {
        return stepsCount;
    }

}
