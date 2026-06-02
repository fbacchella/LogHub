package loghub.events;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import loghub.BuildableConnectionContext;
import loghub.ConnectionContext;
import loghub.cloners.DeepCloner;
import loghub.cloners.NotClonableException;
import loghub.decoders.Decoder;
import lombok.Getter;

@JsonSerialize(using = LockedConnectionContext.Serializer.class)
public class LockedConnectionContext implements ConnectionContext<Object>, Cloneable {

    static {
        DeepCloner.register(LockedConnectionContext.class, o -> (LockedConnectionContext) o.clone());
    }

    @Getter
    private final Object localAddress;
    @Getter
    private final Object remoteAddress;
    @Getter
    private final Principal principal;
    @Getter
    private final Runnable onAcknowledge;
    private final Map<String, ?> properties;

    LockedConnectionContext(ConnectionContext<?> context) {
        this(context, false);
    }

    private LockedConnectionContext(ConnectionContext<?> context, boolean clone) {
        try {
            localAddress = context.getLocalAddress();
            remoteAddress = context.getRemoteAddress();
            principal = context.getPrincipal();
            onAcknowledge = clone ? () -> {} : context.getOnAcknowledge();
            switch (context) {
            case BuildableConnectionContext<?> bcc -> properties = Map.copyOf(DeepCloner.clone(bcc.getProperties()));
            case LockedConnectionContext lcc ->
                // Already locked and copied, simple copy
                properties = lcc.properties;
            default -> properties = Map.of();
            }
        } catch (NotClonableException e) {
            throw new IllegalArgumentException("Not clonable context", e);
        }
    }

    @Override
    public void acknowledge() {
        onAcknowledge.run();
    }

    @Override
    public Optional<Decoder> getDecoder() {
        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getProperty(String property) {
        return (Optional<T>) Optional.ofNullable(properties.get(property));
    }

    public boolean containsValue(Object value) {
        return (principal != null && principal.equals(value))
                       || (localAddress != null && localAddress.equals(value))
                       || (remoteAddress != null && remoteAddress.equals(value))
                       || properties.containsValue(value);
    }

    public int size() {
        return properties.size() + 3;
    }

    public Map<String, Object> getProperties() {
        Map<String, Object> newProps = HashMap.newHashMap(properties.size() + 4);
        newProps.put("principal", principal);
        newProps.put("localAddress", localAddress);
        newProps.put("remoteAddress", remoteAddress);
        newProps.putAll(properties);
        return newProps;
    }

    /**
     * Only one acknowledgement is allowed for each event, so clone drop the current runnable.
     *
     * @return a new context without acknowledge active
     */
    @Override
    @SuppressWarnings({"java:S2975", "java:S1182"})
    public Object clone() {
        return new LockedConnectionContext(this, true);
    }

    static class Serializer extends JsonSerializer<LockedConnectionContext> {
        @Override
        public void serialize(LockedConnectionContext value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();

            gen.writeObjectField("localAddress", value.getLocalAddress());
            gen.writeObjectField("remoteAddress", value.getRemoteAddress());

            Principal p = value.getPrincipal();
            if (p != null && ! p.getName().isBlank()) {
                gen.writeObjectField("principal", p);
            }

            Map<String, ?> props = value.properties;
            if (props != null && !props.isEmpty()) {
                for (Map.Entry<String, ?> e : props.entrySet()) {
                    String key = e.getKey();
                    Object v = e.getValue();
                    if ("localAddress".equals(key) || "remoteAddress".equals(key) || "principal".equals(key)) {
                        gen.writeFieldName("property_" + key);
                        serializers.defaultSerializeValue(v, gen);
                    } else {
                        gen.writeFieldName(key);
                        serializers.defaultSerializeValue(v, gen);
                    }
                }
            }

            gen.writeEndObject();
        }
    }

}
