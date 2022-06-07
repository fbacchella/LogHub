package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.Expression;

public class ExpressionMetaClass extends DelegatingMetaClass {

    public ExpressionMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public ExpressionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        if ("protect".equals(methodName)) {
            return ((Expression) object).protect(arguments[0].toString(), arguments[1]);
        } else if ("stringMethod".equals(methodName)) {
            return ((Expression) object).stringMethod(arguments[0].toString(), arguments[1]);
        } else if ("nullfilter".equals(methodName)) {
            return ((Expression) object).nullfilter(arguments[0], arguments[1].toString());
        } else if ("compare".equals(methodName)) {
            return ((Expression) object).compare(arguments[0].toString(), arguments[1], arguments[2]);
        } else if ("getIterableIndex".equals(methodName)) {
            return ((Expression) object).getIterableIndex(arguments[0], (Integer)arguments[1]);
        } else {
            assert false;
            return super.invokeMethod(object, methodName, arguments);
        }
    }
    
    
}
