package loghub.groovy;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;

public class ObjectMetaClass extends DelegatingMetaClass {
    public ObjectMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods groovyOp = GroovyMethods.resolveSymbol(methodName);
        return super.invokeMethod(object, groovyOp.groovyMethod, arguments);
    }

}
