package loghub.groovy;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.groovy.runtime.typehandling.NumberMath;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NumberMetaClass extends DelegatingMetaClass {

    public NumberMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods method = GroovyMethods.resolveGroovyName(methodName);
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            if (method == GroovyMethods.COMPARE_TO) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arguments.length == 1 && arguments[0] instanceof Number) {
            Number arg1 = (Number) object;
            Number arg2 = (Number) arguments[0];
            Number value = null;
            switch (method) {
            case DIV:
                value = NumberMath.divide(arg1, arg2);
                break;
            case MULTIPLY:
                value = NumberMath.multiply(arg1, arg2);
                break;
            case XOR:
                value = NumberMath.xor(arg1, arg2);
                break;
            case LEFT_SHIFT:
                value = NumberMath.leftShift(arg1, arg2);
                break;
            case RIGHT_SHIFT:
                value = NumberMath.rightShift(arg1, arg2);
                break;
            case RIGHT_SHIFT_UNSIGNED:
                value = NumberMath.rightShiftUnsigned(arg1, arg2);
                break;
            case PLUS:
                value = NumberMath.add(arg1, arg2);
                break;
            case MINUS:
                value = NumberMath.subtract(arg1, arg2);
                break;
            case AND:
                value = NumberMath.and(arg1, arg2);
                break;
            case OR:
                value = NumberMath.or(arg1, arg2);
                break;
            case MOD:
                value = NumberMath.mod(arg1, arg2);
                break;
            case POWER:
                value = Double.NaN;
                BigInteger power = NumberMath.toBigInteger(arg2);
                if (power.bitLength() < 32) {
                    try {
                        value = NumberMath.toBigDecimal(arg1).pow(arg2.intValue());
                    } catch (ArithmeticException ex) {
                        // value will stay Double.NaN
                    }
                }
                break;
            case COMPARE_TO:
                value = NumberMath.compareTo(arg1, arg2);
                break;
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
        } else if (method == GroovyMethods.COMPARE_TO) {
            throw new ClassCastException(arguments[0] + " not a number");
        } else if (method == GroovyMethods.BITWISE_NEGATE) {
            return NumberMath.bitwiseNegate((Number) object);
        } else {
            for (Object argument : arguments) {
                if (argument instanceof NullOrMissingValue) {
                    throw IgnoredEventException.INSTANCE;
                }
            }
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
