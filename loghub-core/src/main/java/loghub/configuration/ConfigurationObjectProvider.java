package loghub.configuration;

import java.util.Map;

public interface ConfigurationObjectProvider<C> {
    C getConfigurationObject(Map<String, Object> properties);
    Class<C> getClassConfiguration();
    String getPrefixFilter();
    default void configure(Properties props) {
        // Nothing to do
    }
}
