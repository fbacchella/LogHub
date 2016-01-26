package loghub.processors;

import java.util.Map;

import loghub.Event;
import loghub.ProcessorException;

public class Mapper extends Etl {

    private Map<Object, Object> map;
    private String field;

    @Override
    public void process(Event event) throws ProcessorException {
        if(! event.containsKey(field)) {
            return;
        }
        Object key = event.get(field);
        if(key == null) {
            return;
        }
        Object value = map.get(key);
        if (value == null) {
            return;
        }
        event.put(field, value);
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
    public String getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String field) {
        this.field = field;
    }

}
