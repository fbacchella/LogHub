package loghub.processors;

import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

public class Test extends Processor {

    @Getter @Setter
    private Expression test;
    private Processor thenTransformer;
    private Processor elseTransformer = new Identity();

    public Processor getThen() {
        return thenTransformer;
    }

    public void setThen(Processor thenTransformer) {
        this.thenTransformer = thenTransformer;
    }

    public Processor getElse() {
        return elseTransformer;
    }

    public void setElse(Processor elseTransformer) {
        this.elseTransformer = elseTransformer;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        boolean testResult;
        try {
            testResult = Boolean.TRUE.equals(test.eval(event));
        } catch (IgnoredEventException e) {
            testResult = false;
        }
        Processor nextTransformer = testResult ? thenTransformer : elseTransformer;
        event.insertProcessor(nextTransformer);
        return testResult;
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public boolean configure(Properties properties) {
        return super.configure(properties) && thenTransformer.configure(properties) && elseTransformer.configure(properties);
    }

}
