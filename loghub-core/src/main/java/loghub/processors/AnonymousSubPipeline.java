package loghub.processors;

import loghub.Pipeline;
import loghub.Processor;
import loghub.SubPipeline;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AnonymousSubPipeline extends Processor implements SubPipeline {

    private Pipeline pipeline;

    @Override
    public boolean process(Event event) {
        assert false;
        return false;
    }

    @Override
    public boolean configure(Properties properties) {
        return pipeline.processors.stream().allMatch(i -> i.configure(properties)) &&
                super.configure(properties);
    }

}
