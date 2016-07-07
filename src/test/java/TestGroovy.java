import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import loghub.Event;

public class TestGroovy {

    @Test
    public void testGroovy1() {
        GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
        Script groovyScript = groovyShell.parse("event.a * 2");
        Binding groovyBinding = new Binding();
        Event event = new Event();
        event.put("a", 1);
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        System.out.println(groovyScript.run().getClass());
    }
    @SuppressWarnings("unchecked")
    @Test
    public void testGroovy2() throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, IOException {
        String script = "event.a.b * 2";

        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class<Script> theParsedClass = groovyClassLoader.parseClass(script);
        Script groovyScript = theParsedClass.newInstance();
        Binding groovyBinding = new Binding();
        Event event = new Event();
        event.put("a", Collections.singletonMap("b", 1));
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);

        System.out.println(groovyScript.run().getClass());

//        System.out.println("methods");
//        Arrays.asList(theParsedClass.getMethods()).forEach( i -> {
//            System.out.println(i);
//        });
//        System.out.println("fields");
//        Arrays.asList(theParsedClass.getFields()).forEach( i -> {
//            System.out.println(i);
//        });
//        System.out.println("interfaces");
//        Arrays.asList(theParsedClass.getInterfaces()).forEach( i -> {
//            System.out.println(i);
//        });
//        Object i = theParsedClass.newInstance();
//        Method m = theParsedClass.getMethod("run");
//        System.out.println(m.invoke(i));
//        groovyClassLoader.close();
    }

}
