package loghub.groovy;

import java.util.HashMap;
import java.util.Map;

public enum GroovyMethods {

    COMPARE_TO("compareTo", null),
    DIV("div", "/"),
    MULTIPLY("multiply", "*"),
    XOR("xor", "^"),
    LEFT_SHIFT("leftShift", "<<"),
    RIGHT_SHIFT("rightShift", ">>"),
    RIGHT_SHIFT_UNSIGNED("rightShiftUnsigned", ">>>"),
    PLUS("plus", "+"),
    MINUS("minus", "-"),
    AND("and", "&"),
    OR("or", "|"),
    MOD("mod", "%"),
    POWER("power", "**"),
    AS_BOOLEAN("asBoolean", null),
    EQUALS("equals", null),
    AS_TYPE("asType", null),
    BITWISE_NEGATE("bitwiseNegate", "~"),
    CLONE("clone", null);

    private static final Map<String, GroovyMethods> mapSymbol = new HashMap<>();
    private static final Map<String, GroovyMethods> mapGroovyMethod = new HashMap<>();
    static {
        for (GroovyMethods go: GroovyMethods.values()) {
            if (go.symbol != null) {
                mapSymbol.put(go.symbol, go);
            }
            mapGroovyMethod.put(go.groovyMethod, go);
        }
    }

    public final String groovyMethod;
    public final String symbol;

    GroovyMethods(String groovyMethod, String symbol) {
        this.groovyMethod = groovyMethod;
        this.symbol = symbol;
    }

    public static GroovyMethods resolveSymbol(String symbol) {
        if (mapSymbol.containsKey(symbol)) {
            return mapSymbol.get(symbol);
        } else {
            throw new UnsupportedOperationException(symbol);
        }
    }

    public static GroovyMethods resolveGroovyName(String groovyMethod) {
        if (mapGroovyMethod.containsKey(groovyMethod)) {
            return mapGroovyMethod.get(groovyMethod);
        } else {
            throw new UnsupportedOperationException(groovyMethod);
        }
    }

}
