package loghub.processors;

import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class Test extends Processor {

    private static final Logger logger = LogManager.getLogger();

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
        try {
            nextTransformer.process(event);
        } catch (ProcessorException e) {
            throw new ProcessorException("test term failed to execute", e);
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
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error("Critical groovy error for expression {}: {}", ifClauseSource, e.getMessage());
            logger.throwing(Level.DEBUG, e);
            return false;
        }
        return super.configure(properties);
    }

}
