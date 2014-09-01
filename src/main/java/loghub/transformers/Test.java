package loghub.transformers;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;
import loghub.Transformer;

public class Test extends Transformer {

    Script ifClause;
    Transformer thenTransformer;
    Transformer elseTransformer = new Identity();

    public String getIf() {
        return ifClause.toString();
    }
    public void setIf(String ifClause) {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        this.ifClause = groovyShell.parse(ifClause);   
    }
    public Transformer getThen() {
        return thenTransformer;
    }
    public void setThen(Transformer thenTransformer) {
        this.thenTransformer = thenTransformer;
    }
    public Transformer getElse() {
        return elseTransformer;
    }
    public void setElse(Transformer elseTransformer) {
        this.elseTransformer = elseTransformer;
    }
    @Override
    public void transform(Event event) {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        ifClause.setBinding(groovyBinding);
        try {
            Boolean testResult = (Boolean) ifClause.run();
            Transformer nextTransformer = testResult ? thenTransformer : elseTransformer;
            nextTransformer.transform(event);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public String getName() {
        return "test";
    }

}
