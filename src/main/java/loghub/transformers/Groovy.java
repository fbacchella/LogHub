package loghub.transformers;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.Map;

import loghub.Event;
import loghub.Transformer;
import loghub.configuration.Beans;

@Beans({"script"})
public class Groovy extends Transformer  {

    private Script groovyScript;
    public Groovy(Map<String, Event> eventQueue) {
        super(eventQueue);
        setName("TransformerGrovy");
    }

    @Override
    public void transform(Event event) {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        try {
            groovyScript.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void setScript(String script) {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        groovyScript = groovyShell.parse(script);   
    }

    public String getScript() {
        return groovyScript.toString();
    }

}
