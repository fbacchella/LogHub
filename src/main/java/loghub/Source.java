package loghub;

import loghub.configuration.Properties;

public interface Source {
    public boolean configure(Properties properties);
    public String getName();
    public void setName(String name);
}
