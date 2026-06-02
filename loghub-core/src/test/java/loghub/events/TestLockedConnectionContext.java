package loghub.events;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.VarFormatter;

class TestLockedConnectionContext {

    public record MockPrincipal(String name) implements Principal, java.io.Serializable {
        @Override public String getName() { return name; }
    }

    @Test
    void testBasic() {
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        LockedConnectionContext locked = MockConnectionContext.builder()
                .onAcknowledge(() -> acknowledged.set(true))
                .build();

        Assertions.assertEquals("local", locked.getLocalAddress());
        Assertions.assertEquals("remote", locked.getRemoteAddress());
        Assertions.assertEquals("user", locked.getPrincipal().getName());
        Assertions.assertFalse(acknowledged.get());
        locked.acknowledge();
        Assertions.assertTrue(acknowledged.get());
        Assertions.assertFalse(locked.getDecoder().isPresent());
    }

    @Test
    void testClone() {
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        LockedConnectionContext locked = MockConnectionContext.builder()
                .onAcknowledge(() -> acknowledged.set(true))
                .build();
        LockedConnectionContext clone = (LockedConnectionContext) locked.clone();

        Assertions.assertEquals(locked.getLocalAddress(), clone.getLocalAddress());
        Assertions.assertEquals(locked.getRemoteAddress(), clone.getRemoteAddress());
        Assertions.assertEquals(locked.getPrincipal(), clone.getPrincipal());

        clone.acknowledge();
        Assertions.assertFalse(acknowledged.get(), "Clone should have an empty onAcknowledge");

        locked.acknowledge();
        Assertions.assertTrue(acknowledged.get(), "Original should still have onAcknowledge");
    }

    @Test
    void testPropertiesFromBuildable() {
        LockedConnectionContext locked = MockConnectionContext.builder()
                .properties(Map.of("key", "value"))
                .build();
        Assertions.assertEquals(Optional.of("value"), locked.getProperty("key"));
    }

    @Test
    void testPropertiesFromLocked() {
        LockedConnectionContext first = MockConnectionContext.builder()
                .properties(Map.of("key", "value"))
                .build();
        LockedConnectionContext second = new LockedConnectionContext(first);

        Assertions.assertEquals(Optional.of("value"), second.getProperty("key"));
    }

    @Test
    void testContainsValue() {
        LockedConnectionContext locked = MockConnectionContext.builder()
                .properties(Map.of("key", "value"))
                .localAddress("myLocal")
                .remoteAddress("myRemote")
                .principal(new MockPrincipal("myUser"))
                .build();
        Assertions.assertTrue(locked.containsValue("value"));
        Assertions.assertTrue(locked.containsValue("myLocal"));
        Assertions.assertTrue(locked.containsValue("myRemote"));
        Assertions.assertTrue(locked.containsValue(new MockPrincipal("myUser")));
        Assertions.assertFalse(locked.containsValue("missing"));
    }

    @Test
    void testSize() {
        LockedConnectionContext locked = MockConnectionContext.builder()
                .properties(Map.of("key", "value"))
                .build();
        // 1 property + localAddress + remoteAddress + principal = 4
        Assertions.assertEquals(4, locked.size());
    }

    @Test
    void testSerialization() throws JsonProcessingException {
        LockedConnectionContext locked = MockConnectionContext.builder()
                .principal(new MockPrincipal("testUser"))
                .properties(Map.of("prop1", "val1"))
                .build();

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(LockedConnectionContext.class, new LockedConnectionContext.Serializer());
        mapper.registerModule(module);

        String json = mapper.writeValueAsString(locked);
        Assertions.assertTrue(json.contains("\"localAddress\":\"local\""));
        Assertions.assertTrue(json.contains("\"remoteAddress\":\"remote\""));
        Assertions.assertTrue(json.contains("\"principal\":{\"name\":\"testUser\"}"));
        Assertions.assertTrue(json.contains("\"prop1\":\"val1\""));
    }

    @Test
    void testVarFormatter() {
        VarFormatter vf = new VarFormatter("${@context%j}");
        EventsFactory factory = new EventsFactory();
        Event ev = factory.newEvent(MockConnectionContext.builder().build());
        Assertions.assertEquals("{\"localAddress\":\"local\",\"remoteAddress\":\"remote\",\"principal\":{\"name\":\"user\"}}", vf.format(ev));
    }

}
