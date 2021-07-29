package groovy.runtime.metaclass.java.lang;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.groovy.runtime.typehandling.NumberMath;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NumberMetaClass extends DelegatingMetaClass {

    public NumberMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public NumberMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            if ("compareTo".equals(methodName)) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arguments.length == 1 && object instanceof Number && arguments[0] instanceof Number){
            Number arg1 = (Number)object;
            Number arg2 = (Number)arguments[0];
            Number value = null;
            switch (methodName) {
            case "div":
                value = NumberMath.divide(arg1, arg2);
                break;
            case "multiply":
                value = NumberMath.multiply(arg1, arg2);
                break;
            case "xor":
                value = NumberMath.xor(arg1, arg2);
                break;
            case "leftShift":
                value = NumberMath.leftShift(arg1, arg2);
                break;
            case "rightShift":
                value = NumberMath.rightShift(arg1, arg2);
                break;
            case "rightShiftUnsigned":
                value = NumberMath.rightShiftUnsigned(arg1, arg2);
                break;
            case "plus":
                value = NumberMath.add(arg1, arg2);
                break;
            case "minus":
                value = NumberMath.subtract(arg1, arg2);
                break;
            case "and":
                value = NumberMath.and(arg1, arg2);
                break;
            case "or":
                value = NumberMath.or(arg1, arg2);
                break;
            case "mod":
                value = NumberMath.mod(arg1, arg2);
                break;
            case "power":
                value = Double.NaN;
                BigInteger power = NumberMath.toBigInteger(arg2);
                if (power.bitLength() < 32) {
                    try {
                        value = NumberMath.toBigDecimal(arg1).pow(arg2.intValue());
                    } catch (ArithmeticException ex) {
                        // value will stay Double.NaN
                    }
                }
            }
            if (NumberMath.isBigDecimal(value)) {
                BigDecimal bd = (BigDecimal) value;
                if (bd.scale() == 0) {
                    value = bd.toBigIntegerExact();
                }
            }
            if (NumberMath.isBigInteger(value)) {
                BigInteger bi = (BigInteger) value;
                if (bi.bitLength() < 32) {
                    value = bi.intValue();
                } else if (bi.bitLength() < 64) {
                    value = bi.longValue();
                }
            }
            return value;
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
