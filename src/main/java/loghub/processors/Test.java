package loghub.processors;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class Test extends Processor {

    Script ifClause;
    Processor thenTransformer;
    Processor elseTransformer = new Identity();

    public String getTest() {
        return ifClause.toString();
    }

    public void setTest(String ifClause) {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        this.ifClause = groovyShell.parse(ifClause);   
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
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        ifClause.setBinding(groovyBinding);
        Boolean testResult = (Boolean) ifClause.run();
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
        return super.configure(properties);
    }

}
