package loghub.processors;

import java.util.Map;

import loghub.Event;
import loghub.ProcessorException;

public class Mapper extends Etl {

    private Map<Object, Object> map;
    private String field[];

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object key = event.applyAtPath((i,j,k) -> i.get(j), field, null);
        if(key == null) {
            return false;
        }
        // Map only uses integer as key, as parsing number only generate integer
        // So ensure the the key is an integer
        // Ignore float/double case, floating point key don't make sense
        if (key instanceof Number && ! (key instanceof Integer) && ! (key instanceof Double) && ! (key instanceof Double)) {
            key = Integer.valueOf(((Number) key).intValue());
        }
        if (! map.containsKey(key)) {
            return false;
        }
        Object value =  map.get(key);
        event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, value, true);
        return true;
    }

    @Override
    public String getName() {
        return null;
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

    /**
     * @return the field
     */
    public String[] getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String[] field) {
        this.field = field;
    }

}
