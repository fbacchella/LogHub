package loghub.processors;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;

public class SubPipeline extends Processor {

    private final Pipeline pipeline;
    public SubPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void process(Event event) {
    }

    @Override
    public String getName() {
        return null;
    }

}
