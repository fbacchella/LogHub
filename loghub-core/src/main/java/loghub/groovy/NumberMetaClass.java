package loghub.groovy;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.groovy.runtime.typehandling.NumberMath;

import groovy.lang.MetaClass;

public class NumberMetaClass extends LoghubMetaClass<Number> {

    public NumberMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeTypedMethod(Number arg1, GroovyMethods method, Object argument) {
        if (argument instanceof Number) {
            Number arg2 = (Number) argument;
            Number value;
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
            default:
                return invokeMethod(arg1, method, argument);
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
            return false;
        } else if (method == GroovyMethods.PLUS && argument instanceof CharSequence){
            return arg1.toString() + argument;
        } else {
            return invokeMethod(arg1, method, argument);
        }
    }

    @Override
    public Object invokeTypedMethod(Number object, GroovyMethods method) {
        if (method == GroovyMethods.BITWISE_NEGATE) {
            return NumberMath.bitwiseNegate(object);
        } else {
            return invokeMethod(object, method);
        }
    }

    @Override
    public boolean isHandledClass(Object o) {
        return o instanceof Number;
    }

}
