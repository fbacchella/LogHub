package loghub.processors;

import loghub.Event;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;

public class Test extends Processor {

    private Expression ifClause;
    private String ifClauseSource;
    private Processor thenTransformer;
    private Processor elseTransformer = new Identity();

    public String getTest() {
        return ifClauseSource;
    }

    public void setTest(String ifClauseSource) {
        this.ifClauseSource = ifClauseSource;
    }

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
        if (ifClause != null) {
            Boolean testResult;
            try {
                testResult = Boolean.TRUE.equals(ifClause.eval(event));
            } catch (IgnoredEventException e) {
                testResult = false;
            }
            Processor nextTransformer = testResult ? thenTransformer : elseTransformer;
            event.insertProcessor(nextTransformer);
            return testResult;
        } else {
            throw event.buildException(errorMessage);
        }
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public boolean configure(Properties properties) {
        thenTransformer.configure(properties);
        elseTransformer.configure(properties);
        try {
            ifClause = new Expression(ifClauseSource, properties.groovyClassLoader, properties.formatters);
        } catch (ExpressionException e) {
            Expression.logError(e, ifClauseSource, logger);
            return false;
        }
        return super.configure(properties);
    }

}
