package loghub;

import java.util.Map;

import loghub.configuration.Beans;

@Beans({"field"})
public abstract class Encoder {

    protected String field;

    public boolean configure(Map<String, Object> properties) {
        return true;
    }

    abstract public byte[] encode(Event event);

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
