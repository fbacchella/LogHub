package loghub.events;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import loghub.NullOrMissingValue;
import loghub.VariablePath;

class TestExtractPaths {

    private final EventsFactory factory = new EventsFactory();

    @Test
    void testEmptyMap() {
        Event event = factory.newTestEvent();
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertTrue(paths.isEmpty(), "Empty map should return no paths");
    }

    @Test
    void testSimpleMap() {
        Event event = factory.newTestEvent();
        event.put("key1", "value1");
        event.put("key2", 2);
        
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertEquals(2, paths.size());
        Assertions.assertTrue(paths.contains(VariablePath.of("key1")));
        Assertions.assertTrue(paths.contains(VariablePath.of("key2")));
    }

    @Test
    void testNestedMap() {
        Event event = factory.newTestEvent();
        Map<String, Object> nested = new HashMap<>();
        nested.put("inner", "value");
        
        event.put("outer", nested);
        event.put("sibling", "val");
        
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertEquals(2, paths.size());
        Assertions.assertTrue(paths.contains(VariablePath.of("outer", "inner")));
        Assertions.assertTrue(paths.contains(VariablePath.of("sibling")));
    }

    @Test
    void testMissingValues() {
        Event event = factory.newTestEvent();
        event.put("present", "value");
        event.put("missing", NullOrMissingValue.MISSING);
        
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertEquals(1, paths.size());
        Assertions.assertTrue(paths.contains(VariablePath.of("present")));
        Assertions.assertFalse(paths.contains(VariablePath.of("missing")));
    }

    @Test
    void testNullValues() {
        Event event = factory.newTestEvent();
        event.put("nullValue", null);
        event.put("nullValueInMap", NullOrMissingValue.NULL);
        
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertEquals(2, paths.size());
        Assertions.assertTrue(paths.contains(VariablePath.of("nullValue")));
        Assertions.assertTrue(paths.contains(VariablePath.of("nullValueInMap")));
    }
    
    @Test
    void testDeeplyNested() {
        Event event = factory.newTestEvent();
        Map<String, Object> level3 = Collections.singletonMap("leaf", "value");
        Map<String, Object> level2 = Collections.singletonMap("mid", level3);
        event.put("top", level2);
        
        List<VariablePath> paths = event.enumerateAllPaths().toList();
        Assertions.assertEquals(1, paths.size());
        Assertions.assertEquals(VariablePath.of("top", "mid", "leaf"), paths.getFirst());
    }

}
