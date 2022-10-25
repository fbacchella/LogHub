package loghub.events;

import java.util.NoSuchElementException;
import java.util.Optional;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class PostSubpipline extends Processor {

    public static final PostSubpipline INSTANCE = new PostSubpipline();
    private PostSubpipline() {
        // Empty
    }

    @Override
    public boolean configure(Properties properties) {
        return true;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        ExecutionStackElement.logger.trace("<-- {}({})", () -> event.getRealEvent().executionStack, () -> event);
        try {
            event.getRealEvent().executionStack.remove().close();
        } catch (NoSuchElementException ex) {
            throw new ProcessorException(event.getRealEvent(), "Empty timer stack, bad state");
        }
        Optional.ofNullable(event.getRealEvent().executionStack.peek()).ifPresent(
                ExecutionStackElement::restart);
        event.getRealEvent().refreshLogger();
        return true;
    }

    @Override
    public String getName() {
        return "postSubpipline";
    }

    @Override
    public String toString() {
        return "postSubpipline";
    }
}
