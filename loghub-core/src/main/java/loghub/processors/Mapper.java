package loghub.processors;

import java.util.Map;

import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Mapper extends Etl {

    public static Etl of(VariablePath lvalue, Map<Object, Object> map, Expression expression) {
        return new Mapper(lvalue, map, expression);
    }

    private final Map<Object, Object> map;
    private final Expression expression;

    public Mapper(VariablePath lvalue, Map<Object, Object> map, Expression expression) {
        super(lvalue);
        this.map = map;
        this.expression = expression;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object key = expression.eval(event);
        if (key == null || key instanceof NullOrMissingValue) {
            throw IgnoredEventException.INSTANCE;
        }
        // Map only uses integer number as key, as parsing number only generate integer
        // So ensure the key is an integer if it's a number
        // Ignore float/double case, floating point key don't make sense
        if (key instanceof Number && ! (key instanceof Integer) && ! (key instanceof Double) && ! (key instanceof Float)) {
            key = ((Number) key).intValue();
        }
        if (! map.containsKey(key)) {
            throw IgnoredEventException.INSTANCE;
        }
        Object value =  map.get(key);
        event.putAtPath(lvalue, value);
        return true;
    }

}
