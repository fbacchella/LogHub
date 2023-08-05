package groovy.runtime.metaclass.java.util;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;

public class MapMetaClass extends DelegatingMetaClass {

    public MapMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object getProperty(Object object, String property) {
        return ((java.util.Map)object).get(property);
    }

}
