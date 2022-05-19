package loghub.sources;

import java.util.Map;

import loghub.configuration.Properties;

public interface Source extends Map<Object, Object> {
    boolean configure(Properties properties);
}
