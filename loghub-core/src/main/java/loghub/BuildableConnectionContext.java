package loghub;

import java.io.Serializable;
import java.security.Principal;
import java.util.Optional;

import loghub.cloners.Immutable;
import loghub.decoders.Decoder;
import lombok.Getter;
import lombok.Setter;

/**
 * A mutable ConnectionContext, to be used in receivers.
 * The EventFactory will discard it and build an immutable version
 * @param <A>
 */
public abstract class BuildableConnectionContext<A> implements ConnectionContext<A> {

    @Immutable
    private static final class EmptyPrincipal implements Principal, Serializable {
        @Override
        public String getName() {
            return "";
        }
    }

    private static final Principal EMPTYPRINCIPAL = new EmptyPrincipal();

    @Immutable
    private static final class EmptyConnectionContext implements ConnectionContext<Object> {
        @Override
        public void acknowledge() {
            // Empty on purpose
        }

        @Override
        public Optional<Decoder> getDecoder() {
            return Optional.empty();
        }

        @Override
        public Object getLocalAddress() {
            return null;
        }

        @Override
        public Object getRemoteAddress() {
            return null;
        }

        @Override
        public Principal getPrincipal() {
            return EMPTYPRINCIPAL;
        }

        @Override
        public Runnable getOnAcknowledge() {
            return () -> {
            };
        }
    }

    public static final ConnectionContext<Object> EMPTY = new EmptyConnectionContext();

    @Getter
    public static final class GenericConnectionContext extends BuildableConnectionContext<Object> {
        private final Object localAddress;
        private final Object remoteAddress;
        public GenericConnectionContext(Object localAddress, Object remoteAddress) {
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
        }
    }

    @Getter @Setter
    protected Principal principal;
    @Setter
    Decoder decoder;
    @Getter @Setter
    Runnable onAcknowledge = () -> {};

    protected BuildableConnectionContext() {
        principal = EMPTYPRINCIPAL;
    }

    public final void acknowledge() {
        onAcknowledge.run();
    }

    public Optional<Decoder> getDecoder() {
        return Optional.ofNullable(decoder);
    }

}
