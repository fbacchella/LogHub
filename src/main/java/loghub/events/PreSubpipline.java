package loghub.events;

import java.util.Optional;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class PreSubpipline extends Processor {

    private final String pipename;

    PreSubpipline(String pipename) {
        this.pipename = pipename;
    }

    @Override
    public boolean configure(Properties properties) {
        return true;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Optional.ofNullable(event.getRealEvent().executionStack.peek()).ifPresent(ExecutionStackElement::pause);
        ExecutionStackElement ctxt = new ExecutionStackElement(pipename);
        event.getRealEvent().executionStack.add(ctxt);
        ExecutionStackElement.logger.trace("--> {}({})", () -> event.getRealEvent().executionStack, () -> event);
        return true;
    }

    @Override
    public String getName() {
        return "preSubpipline(" + pipename + ")";
    }

    @Override
    public String toString() {
        return "preSubpipline(" + pipename + ")";
    }
}
