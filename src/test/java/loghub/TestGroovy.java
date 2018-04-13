package loghub;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

public class TestGroovy {

    @Test
    public void testGroovy1() {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        Script groovyScript = groovyShell.parse("event.a * 2");
        Binding groovyBinding = new Binding();
        Event event = new EventInstance(ConnectionContext.EMPTY);
        event.put("a", 1);
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        Integer a = (Integer) groovyScript.run();
        Assert.assertEquals(2, a.intValue());
    }

    @Test
    public void testGroovy2() throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, IOException {
        String script = "event.a.b * 2";

        try(GroovyClassLoader groovyClassLoader = new GroovyClassLoader()) {
            @SuppressWarnings("unchecked")
            Class<Script> theParsedClass = groovyClassLoader.parseClass(script);
            Script groovyScript = theParsedClass.newInstance();
            Binding groovyBinding = new Binding();
            Event event = new EventInstance(ConnectionContext.EMPTY);
            event.put("a", Collections.singletonMap("b", 1));
            groovyBinding.setVariable("event", event);
            groovyScript.setBinding(groovyBinding);
            Integer b = (Integer) groovyScript.run();
            Assert.assertEquals(2, b.intValue());
        }

    }

}
