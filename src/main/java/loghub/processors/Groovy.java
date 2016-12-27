package loghub.processors;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Beans;

@Beans({"script"})
public class Groovy extends Processor  {

    private Script groovyScript;

    @Override
    public boolean process(Event event) throws ProcessorException {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        try {
            return Boolean.TRUE.equals(groovyScript.run());
        } catch (Exception e) {
            throw event.buildException("groovy script failed", e);
        }
    }

    public void setScript(String script) {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        groovyScript = groovyShell.parse(script);
    }

    public String getScript() {
        return groovyScript.toString();
    }

    @Override
    public String getName() {
        return "groovy";
    }

}
