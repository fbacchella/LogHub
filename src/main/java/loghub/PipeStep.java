package loghub;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;
import loghub.processors.Drop;
import loghub.processors.Forker;

public class PipeStep extends Thread {


    private static final Logger logger = LogManager.getLogger();

    private NamedArrayBlockingQueue queueIn;
    private NamedArrayBlockingQueue queueOut;

    private final List<Processor> processors = new ArrayList<>();

    public PipeStep() {
        setDaemon(true);
    }

    public PipeStep(String name, int numStep, int width) {
        setDaemon(true);
        setName(name + "@" + numStep + "." + width);
    }

    public boolean configure(final Properties properties) {
        return processors.stream().allMatch(i -> i.configure(properties));
    }

    public void start(NamedArrayBlockingQueue endpointIn, NamedArrayBlockingQueue endpointOut) {
        logger.debug("starting {}", this);

        this.queueIn = endpointIn;
        this.queueOut = endpointOut;

        super.start();
    }

    public void addProcessor(Processor t) {
        processors.add(t);
    }

    public void run() {
        String threadName = getName();
        try {
            logger.debug("waiting on {}", queueIn.name);
            while(! isInterrupted()) {
                Event event = queueIn.take();
                logger.trace("{} received event {}", queueIn.name, event);
                EventWrapper wevent = new EventWrapper(event);
                for(Processor p: processors) {
                    setName(threadName + "-" + p.getName());
                    if(p instanceof Forker) {
                        ((Forker) p).fork(event);
                    } else if(p instanceof Drop) {
                        event.dropped = true;
                        continue;
                    } else {
                        wevent.setProcessor(p);
                        if(p.isprocessNeeded(wevent)) {
                            try {
                                p.process(wevent);
                            } catch (ProcessorException e) {
                                Stats.newError(e);
                            } catch (Exception e) {
                                logger.error("failed to transform event with unmanaged error {}: {}", event, e.getMessage());
                                logger.throwing(Level.ERROR, e);
                                event.dropped = true;
                            }
                        }
                    }
                }
                setName(threadName);
                if( ! event.dropped) {
                    if( ! queueOut.offer(event) ) {
                        Stats.dropped.incrementAndGet();
                        logger.error("send failed for {}, destination blocked", event);
                    };
                    logger.trace("{} send event {}", () -> queueOut, () -> event);
                } else {
                    logger.error("{} dropped event {}", () -> queueOut, () -> event);
                }
            }
        } catch (InterruptedException e) {
            logger.debug("stop waiting on {}", queueIn.name);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "." + processors.toString();
    }

}
