package loghub.events;

import java.util.Optional;

import loghub.Pipeline;
import loghub.Processor;
import loghub.configuration.Properties;

public class PreSubpipline extends Processor {

    private final Pipeline pipe;

    PreSubpipline(Pipeline pipe) {
        this.pipe = pipe;
    }

    @Override
    public boolean configure(Properties properties) {
        return true;
    }

    @Override
    public boolean process(Event event) {
        Optional.ofNullable(event.getRealEvent().executionStack.peek()).ifPresent(ExecutionStackElement::pause);
        ExecutionStackElement ctxt = new ExecutionStackElement(pipe);
        event.getRealEvent().executionStack.add(ctxt);
        ExecutionStackElement.logger.trace("--> {}({})", () -> event.getRealEvent().executionStack, () -> event);
        return true;
    }

    @Override
    public String getName() {
        return "preSubpipline(" + pipe.getName() + ")";
    }

    @Override
    public String toString() {
        return "preSubpipline(" + pipe.getName() + ")";
    }

}
