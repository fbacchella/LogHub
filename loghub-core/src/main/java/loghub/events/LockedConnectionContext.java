package loghub.events;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import loghub.BuildableConnectionContext;
import loghub.ConnectionContext;
import loghub.cloners.DeepCloner;
import loghub.cloners.NotClonableException;
import loghub.decoders.Decoder;
import lombok.Getter;

class LockedConnectionContext implements ConnectionContext<Object>, Cloneable {

    static {
        DeepCloner.register(LockedConnectionContext.class, o -> ((LockedConnectionContext)o).clone());
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

    /**
     * Only one acknowledge is allowed for each event, so clone drop the current runnable.
     *
     * @return a new context without acknowledge active
     */
    @Override
    @SuppressWarnings({"java:S2975", "java:S1182"})
    public Object clone() {
        return new LockedConnectionContext(this, true);
    }

}
