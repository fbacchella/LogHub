package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.processors.NamedSubPipeline;

class EventInstance extends Event {

    private final static Logger logger = LogManager.getLogger();


    private final EventWrapper wevent;
    private final LinkedList<Processor> processors = new LinkedList<>();

    EventInstance() {
        wevent = new EventWrapper(this);
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
            Event newEvent = (Event) new ObjectInputStream(bais).readObject();
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
        try {
            Processor p = processors.pop();
            wevent.setProcessor(p);
            // We just remove the last processor
            // If it's a named pipeline, it become the main pipeline
            if (processors.size() == 0 && p instanceof NamedSubPipeline) {
                NamedSubPipeline named = (NamedSubPipeline) p;
                this.mainPipeline = named.getPipeline().getName();
            }
            return p;
        } catch (NoSuchElementException e) {
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
    public void inject(Pipeline pipeline, BlockingQueue<Event> mainqueue) {
        mainPipeline = pipeline.getName();
        appendProcessors(pipeline.processors);
        mainqueue.offer(this.wevent);
    }

    public void inject(BlockingQueue<Event> mainqueue) {
        mainqueue.offer(this);
    }

    private void inject(List<Processor> newProcessors, boolean append) {
        ListIterator<Processor> i = newProcessors.listIterator(append ? newProcessors.size() : 0);
        while(append ? i.hasNext() : i.hasPrevious()) {
            Processor p = append ? i.next() : i.previous();
            inject(p, append);
        }
    }

    private void inject(Processor p, boolean append) {
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

}
