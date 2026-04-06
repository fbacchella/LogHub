package loghub.events;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.BuildableConnectionContext;
import loghub.ConnectionContext;
import loghub.VarFormatter;
import loghub.decoders.Decoder;

class TestLockedConnectionContext {

    public static class MockPrincipal implements Principal {
        private final String name;
        MockPrincipal(String name) { this.name = name; }
        @Override public String getName() { return name; }
    }

    private static class MockConnectionContext implements ConnectionContext<Object> {
        private final Object localAddress = "local";
        private final Object remoteAddress = "remote";
        private final Principal principal = new MockPrincipal("user");
        private final AtomicBoolean acknowledged = new AtomicBoolean(false);
        private final Runnable onAcknowledge = () -> acknowledged.set(true);

        @Override
        public void acknowledge() { onAcknowledge.run(); }
        @Override
        public Optional<Decoder> getDecoder() { return Optional.empty(); }
        @Override
        public Object getLocalAddress() { return localAddress; }
        @Override
        public Object getRemoteAddress() { return remoteAddress; }
        @Override
        public Principal getPrincipal() { return principal; }
        @Override
        public Runnable getOnAcknowledge() { return onAcknowledge; }
        @Override
        public <T> Optional<T> getProperty(String property) { return Optional.empty(); }
    }

    @Test
    void testBasic() {
        MockConnectionContext mock = new MockConnectionContext();
        LockedConnectionContext locked = new LockedConnectionContext(mock);

        Assertions.assertEquals("local", locked.getLocalAddress());
        Assertions.assertEquals("remote", locked.getRemoteAddress());
        Assertions.assertEquals("user", locked.getPrincipal().getName());
        Assertions.assertFalse(mock.acknowledged.get());
        locked.acknowledge();
        Assertions.assertTrue(mock.acknowledged.get());
        Assertions.assertFalse(locked.getDecoder().isPresent());
    }

    @Test
    void testClone() {
        MockConnectionContext mock = new MockConnectionContext();
        LockedConnectionContext locked = new LockedConnectionContext(mock);
        LockedConnectionContext clone = (LockedConnectionContext) locked.clone();

        Assertions.assertEquals(locked.getLocalAddress(), clone.getLocalAddress());
        Assertions.assertEquals(locked.getRemoteAddress(), clone.getRemoteAddress());
        Assertions.assertEquals(locked.getPrincipal(), clone.getPrincipal());

        clone.acknowledge();
        Assertions.assertFalse(mock.acknowledged.get(), "Clone should have an empty onAcknowledge");

        locked.acknowledge();
        Assertions.assertTrue(mock.acknowledged.get(), "Original should still have onAcknowledge");
    }

    @Test
    void testPropertiesFromBuildable() {
        BuildableConnectionContext<Object> bcc = new BuildableConnectionContext<Object>() {
            @Override
            public Map<String, Object> getProperties() {
                Map<String, Object> props = new HashMap<>();
                props.put("key", "value");
                return props;
            }
            @Override public Object getLocalAddress() { return "l"; }
            @Override public Object getRemoteAddress() { return "r"; }
        };

        LockedConnectionContext locked = new LockedConnectionContext(bcc);
        Assertions.assertEquals(Optional.of("value"), locked.getProperty("key"));
    }

    @Test
    void testPropertiesFromLocked() {
        BuildableConnectionContext<Object> bcc = new BuildableConnectionContext<Object>() {
            @Override
            public Map<String, Object> getProperties() {
                return Collections.singletonMap("key", "value");
            }
            @Override public Object getLocalAddress() { return "l"; }
            @Override public Object getRemoteAddress() { return "r"; }
        };

        LockedConnectionContext first = new LockedConnectionContext(bcc);
        LockedConnectionContext second = new LockedConnectionContext(first);

        Assertions.assertEquals(Optional.of("value"), second.getProperty("key"));
    }

    @Test
    void testSerialization() throws JsonProcessingException {
        BuildableConnectionContext<Object> bcc = new BuildableConnectionContext<>() {
            @Override
            public Map<String, Object> getProperties() {
                Map<String, Object> props = new HashMap<>();
                props.put("prop1", "val1");
                props.put("localAddress", "overriddenLocal");
                return props;
            }
            @Override public Object getLocalAddress() { return "localAddr"; }
            @Override public Object getRemoteAddress() { return "remoteAddr"; }
        };
        bcc.setPrincipal(new MockPrincipal("testUser"));

        LockedConnectionContext locked = new LockedConnectionContext(bcc);

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(LockedConnectionContext.class, new LockedConnectionContext.Serializer());
        mapper.registerModule(module);

        String json = mapper.writeValueAsString(locked);
        Assertions.assertTrue(json.contains("\"localAddress\":\"localAddr\""));
        Assertions.assertTrue(json.contains("\"remoteAddress\":\"remoteAddr\""));
        Assertions.assertTrue(json.contains("\"principal\":{\"name\":\"testUser\"}"));
        Assertions.assertTrue(json.contains("\"prop1\":\"val1\""));
        Assertions.assertTrue(json.contains("\"property_localAddress\":\"overriddenLocal\""));
    }

    @Test
    void testVarFormatter() {
        VarFormatter vf = new VarFormatter("${@context%j}");
        EventsFactory factory = new EventsFactory();
        Event ev = factory.newEvent(new MockConnectionContext());
        Assertions.assertEquals("{\"localAddress\":\"local\",\"remoteAddress\":\"remote\",\"principal\":{\"name\":\"user\"}}", vf.format(ev));
    }

}
