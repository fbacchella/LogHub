package loghub.processors;

import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
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
    public void process(Event event) throws ProcessorException {
        Boolean testResult = Boolean.TRUE.equals(ifClause.eval(event, Collections.emptyMap()));
        Processor nextTransformer = testResult ? thenTransformer : elseTransformer;
        event.insertProcessor(nextTransformer);
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
            Throwable cause = e.getCause();
            if (cause instanceof CompilationFailedException) {
                logger.error("Groovy compilation failed for expression {}: {}", ifClauseSource, e.getMessage());
                return false;
            } else {
                logger.error("Critical groovy error for expression {}: {}", ifClauseSource, e.getMessage());
                logger.throwing(Level.DEBUG, e.getCause());
                return false;
            }
        }
        return super.configure(properties);
    }

}
