package loghub.events;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import loghub.BuildableConnectionContext;
import loghub.decoders.Decoder;
import loghub.events.TestLockedConnectionContext.MockPrincipal;

public class MockConnectionContext extends BuildableConnectionContext<Object> {
    private final Object localAddress;
    private final Object remoteAddress;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);
    private final Runnable onAcknowledge;
    private final Map<String, Object> properties;

    private MockConnectionContext(Builder builder) {
        this.localAddress = builder.localAddress;
        this.remoteAddress = builder.remoteAddress;
        this.onAcknowledge = builder.onAcknowledge != null ? builder.onAcknowledge : () -> acknowledged.set(true);
        this.properties = builder.properties;
        setPrincipal(builder.principal);
    }

    public boolean isAcknowledged() {
        return acknowledged.get();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Principal principal = new MockPrincipal("user");
        private Runnable onAcknowledge = null;
        private Map<String, Object> properties = Map.of();
        private Object localAddress = "local";
        private Object remoteAddress = "remote";

        public Builder localAddress(Object localAddress) {
            this.localAddress = localAddress;
            return this;
        }

        public Builder remoteAddress(Object remoteAddress) {
            this.remoteAddress = remoteAddress;
            return this;
        }

        public Builder principal(Principal principal) {
            this.principal = principal;
            return this;
        }

        public Builder onAcknowledge(Runnable onAcknowledge) {
            this.onAcknowledge = onAcknowledge;
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.properties = properties;
            return this;
        }
        public LockedConnectionContext build() {
            return new LockedConnectionContext(new MockConnectionContext(this));
        }
    }

    @Override
    public Optional<Decoder> getDecoder() {
        return Optional.empty();
    }

    @Override
    public Object getLocalAddress() {
        return localAddress;
    }

    @Override
    public Object getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public Runnable getOnAcknowledge() {
        return onAcknowledge;
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getProperty(String property) {
        return Optional.ofNullable((T) properties.get(property));
    }
}
