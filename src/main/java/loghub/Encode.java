package loghub;

import java.util.Map;

import loghub.configuration.Beans;

@Beans({"field"})
public abstract class Encode {

    protected String field;
    
    public void configure(Map<String, Object> properties) {
        
    }
    
    abstract public byte[] encode(Event event);

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
    
}
