package loghub.events;

import java.security.Principal;
import java.util.Optional;

import loghub.ConnectionContext;
import loghub.cloners.DeepCloner;
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

    LockedConnectionContext(ConnectionContext<?> context) {
        this(context, false);
    }

    private LockedConnectionContext(ConnectionContext<?> context, boolean clone) {
        localAddress = context.getLocalAddress();
        remoteAddress = context.getRemoteAddress();
        principal = context.getPrincipal();
        onAcknowledge = clone ? () -> {} : context.getOnAcknowledge();
    }

    @Override
    public void acknowledge() {
        onAcknowledge.run();
    }

    @Override
    public Optional<Decoder> getDecoder() {
        return Optional.empty();
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
