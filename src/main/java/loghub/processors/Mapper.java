package loghub.processors;

import java.util.Map;

import loghub.Event;
import loghub.Event.Action;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import lombok.Getter;
import lombok.Setter;

public class Mapper extends Etl {

    private Map<Object, Object> map;

    @Getter @Setter
    private Expression expression;

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object key = expression.eval(event);
        if (key == null) {
            return false;
        } else if (key == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        }
        // Map only uses integer number as key, as parsing number only generate integer
        // So ensure the the key is an integer
        // Ignore float/double case, floating point key don't make sense
        if (key instanceof Number && ! (key instanceof Integer) && ! (key instanceof Double) && ! (key instanceof Float)) {
            key = Integer.valueOf(((Number) key).intValue());
        }
        if (! map.containsKey(key)) {
            return false;
        }
        Object value =  map.get(key);
        event.applyAtPath(Action.PUT, lvalue, value, true);
        return true;
    }

    /**
     * @return the map
     */
    public Map<Object, Object> getMap() {
        return map;
    }

    /**
     * @param map the map to set
     */
    public void setMap(Map<Object, Object> map) {
        this.map = map;
    }

}
