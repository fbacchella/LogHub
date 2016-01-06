package loghub.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;
import loghub.Processor;
import loghub.configuration.Beans;

@Beans({"script"})
public class Groovy extends Processor  {

    private static final Logger logger = LogManager.getLogger();

    private Script groovyScript;

    @Override
    public void process(Event event) {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        try {
            groovyScript.run();
        } catch (Exception e) {
            logger.error("script failed: {}", e.getMessage());
            logger.catching(e);
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
