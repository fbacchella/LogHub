package groovy.runtime.metaclass.loghub;

import java.util.Map;
import java.util.function.Function;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;

public class LambdaPropertyMetaClass extends DelegatingMetaClass {

    Map<String, Function<Object, Object>> properties;

    public LambdaPropertyMetaClass(MetaClass delegate, Map<String, Function<Object, Object>> s) {
        super(delegate);
        this.properties = s;
    }

    @Override
    public Object getProperty(Object object, String property) {
        return properties.getOrDefault(property, o -> super.getProperty(o, property)).apply(object);
    }

}
