package loghub.processors;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;
import loghub.Processor;

public class Test extends Processor {

    Script ifClause;
    Processor thenTransformer;
    Processor elseTransformer = new Identity();

    public String getIf() {
        return ifClause.toString();
    }
    public void setIf(String ifClause) {
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
    public void process(Event event) {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        ifClause.setBinding(groovyBinding);
        try {
            Boolean testResult = (Boolean) ifClause.run();
            Processor nextTransformer = testResult ? thenTransformer : elseTransformer;
            nextTransformer.process(event);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public String getName() {
        return "test";
    }

}
