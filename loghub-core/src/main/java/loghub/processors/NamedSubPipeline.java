package loghub.processors;

import loghub.Pipeline;
import loghub.Processor;
import loghub.SubPipeline;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

public class NamedSubPipeline extends Processor implements SubPipeline {

    @Setter
    @Getter
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
        if (pipe == null) {
            logger.error("pipeline '{}' not found", pipeRef);
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public Pipeline getPipeline() {
        return pipe;
    }

}
