package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.VarFormatter;

public class VarFormatterMetaClass extends DelegatingMetaClass {

    public VarFormatterMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        VarFormatter vf = (VarFormatter) object;
        switch(methodName) {
        case "format": return vf.format(arguments[0]);
        default: {
            return super.invokeMethod(object, methodName, arguments);
        }
        }
    }

}
