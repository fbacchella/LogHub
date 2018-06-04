package loghub;

import java.util.Map;

import loghub.configuration.Properties;

public interface Source extends Map<Object, Object> {
    public boolean configure(Properties properties);
    public String getName();
    public void setName(String name);
}
