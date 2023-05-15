package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.Expression;
import loghub.Helpers;

public class ExpressionMetaClass extends DelegatingMetaClass {

    public ExpressionMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public ExpressionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        assert object instanceof Expression;
        Expression ex = (Expression) object;
        switch (methodName) {
        case "protect": return ex.protect(arguments[0].toString(), arguments[1]);
        case "stringMethod": return ex.stringMethod(arguments[0].toString(), arguments[1]);
        case "nullfilter": return ex.nullfilter(arguments[0]);
        case "compare": return ex.compare(arguments[0].toString(), arguments[1], arguments[2]);
        case "getIterableIndex": return ex.getIterableIndex(arguments[0], (Integer)arguments[1]);
        case "isEmpty": return Helpers.isEmpty(arguments[0]);
        case "regex": return ex.regex(arguments[0], arguments[1].toString(), arguments[2].toString());
        default:
            assert false;
            return super.invokeMethod(object, methodName, arguments);
        }
    }
    
    
}
