package loghub;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"field"})
public abstract class Encoder {

    protected String field;

    public boolean configure(Properties properties) {
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
