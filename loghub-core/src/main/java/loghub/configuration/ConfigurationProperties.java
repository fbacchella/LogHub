package loghub.configuration;

import java.util.Map;
import java.util.function.BiConsumer;

public class ConfigurationProperties {

    private final Map<String, Object> properties;

    public ConfigurationProperties(Map<String, Object> properties) {
        this.properties =  Map.copyOf(properties);
    }

    public ConfigurationProperties() {
        this.properties = Map.of();
    }

    public int size() {
        return properties.size();
    }

    public void forEach(BiConsumer<String, Object> action) {
        properties.forEach(action);
    }

    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public Object get(String key) {
        return properties.get(key);

    }

}
