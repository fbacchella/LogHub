package loghub.processors;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;
import loghub.SubPipeline;
import loghub.configuration.Properties;

public class NamedSubPipeline extends Processor implements SubPipeline {

    private String pipeRef;
    private Pipeline pipe;

    @Override
    public boolean process(Event event) {
        assert false;
        return true;
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
