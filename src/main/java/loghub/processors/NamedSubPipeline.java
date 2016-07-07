package loghub.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;
import loghub.SubPipeline;
import loghub.configuration.Properties;

public class NamedSubPipeline extends Processor implements SubPipeline {

    private static final Logger logger = LogManager.getLogger();

    private String pipeRef;
    private Pipeline pipe;

    @Override
    public void process(Event event) {
        assert false;
//        try {
//            pipe.inQueue.put(event);
//            pipe.outQueue.take();
//        } catch (InterruptedException e) {
//        }
    }

    @Override
    public String getName() {
        return "piperef";
    }

    @Override
    public boolean configure(Properties properties) {
        pipe = properties.namedPipeLine.get(pipeRef);
        if(pipe == null) {
            logger.error("pipeline '{}' not found", pipeRef);
            return false;
        }
        return super.configure(properties);
    }

    public String getPipeRef() {
        return pipeRef;
    }

    public void setPipeRef(String pipeRef) {
        this.pipeRef = pipeRef;
    }

    @Override
    public Pipeline getPipeline() {
        return pipe;
    }

}
