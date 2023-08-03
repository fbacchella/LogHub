package groovy.runtime.metaclass.loghub;

import org.codehaus.groovy.runtime.wrappers.PojoWrapper;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.Expression;
import loghub.Helpers;

public class ExpressionMetaClass extends DelegatingMetaClass {

    public ExpressionMetaClass(Class<?> theClass) {
        super(theClass);
    }

    @SuppressWarnings("unused")
    public ExpressionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] instanceof PojoWrapper) {
                arguments[i] = ((PojoWrapper)arguments[i]).unwrap();
            }
        }
        Expression ex = (Expression) object;
        switch (methodName) {
        case "protect": return ex.protect(arguments[0].toString(), arguments[1]);
        case "stringFunction": return ex.stringFunction(arguments[0].toString(), arguments[1]);
        case "join": return ex.join(arguments[0], arguments[1]);
        case "split": return ex.split(arguments[0], arguments[1]);
        case "nullfilter": return ex.nullfilter(arguments[0]);
        case "compare": return ex.compare(arguments[0].toString(), arguments[1], arguments[2]);
        case "getIterableIndex": return ex.getIterableIndex(arguments[0], (Integer)arguments[1]);
        case "isEmpty": return ex.isEmpty(arguments[0]);
        case "regex": return ex.regex(arguments[0], arguments[1].toString(), arguments[2].toString());
        case "in": return ex.in(arguments[0].toString(), arguments[1], arguments[2]);
        case "instanceof": return ex.instanceOf(arguments[0].toString(), arguments[1], arguments[2]);
        case "newCollection": return ex.newCollection(arguments[0].toString());
        case "asCollection": return ex.asCollection(arguments[0].toString(), arguments[1]);
        default:
            assert false;
            return super.invokeMethod(object, methodName, arguments);
        }
    }
    
    
}
