package groovy.runtime.metaclass.loghub;

import groovy.lang.MetaClass;
import loghub.VarFormatter;

public class VarFormatterMetaClass extends DelegatingMetaClass {

    public VarFormatterMetaClass(Class<?> theClass) {
        super(theClass);
    }

    @SuppressWarnings("unused")
    public VarFormatterMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        VarFormatter vf = (VarFormatter) object;
        switch(methodName) {
        case "format": return vf.format(arguments[0]);
        default: {
            assert false;
            return super.invokeMethod(object, methodName, arguments);
        }
        }
    }

}
