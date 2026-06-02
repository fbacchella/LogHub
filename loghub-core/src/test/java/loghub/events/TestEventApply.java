package loghub.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import loghub.IgnoredEventException;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.VariablePath;

class TestEventApply {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test
    void testPath() {
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("a.b.c"), 1, true);
        e.put("d", 2);
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("e"), 3, true);
        e.applyAtPath(Event.Action.PUT, VariablePath.ofMeta("f"), 4, true);
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("h"), Map.of(), true);
        Assertions.assertEquals(4, e.size());

        applyAction(e, true, Event.Action.CONTAINSVALUE, 2);
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1, "a");
        applyAction(e, true, Event.Action.CONTAINSVALUE, 1, "a", "b");
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1, "a", "b", "c");
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1, "i");

        applyAction(e, false, Event.Action.CONTAINS, null);
        applyAction(e, true, Event.Action.CONTAINS, null, "a");
        applyAction(e, true, Event.Action.CONTAINS, null, "a", "b");
        applyAction(e, true, Event.Action.CONTAINS, null, "a", "b", "c");
        applyAction(e, true, Event.Action.CONTAINS, null, "d");
        applyAction(e, false, Event.Action.CONTAINS, null, "d", "e");

        applyAction(e, 4, Event.Action.SIZE, null);
        applyAction(e, 1, Event.Action.SIZE, null, "a", "b");
    }

    @Test
    void testPathActions() {
        Event e = factory.newEvent();
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("a.b.c"), 1, true);
        e.put("d", 2);
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("e"), 3, true);
        e.applyAtPath(Event.Action.PUT, VariablePath.ofMeta("f"), 4, true);

        applyAction(e, 1, Event.Action.GET, null, "a", "b", "c");
        applyAction(e, NullOrMissingValue.MISSING, Event.Action.GET, null, "a", "b", "g");
        applyAction(e, 2, Event.Action.GET, null, "d");
        applyAction(e, 3, Event.Action.GET, null, "e");
        applyAction(e, NullOrMissingValue.MISSING, Event.Action.GET, null, "d", "e");
        Assertions.assertEquals(4, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("f"), null));

        applyAction(e, 1, Event.Action.PUT, 2, "a", "b", "c");
        applyAction(e, 2, Event.Action.REMOVE, null, "a", "b", "c");

        applyAction(e, NullOrMissingValue.MISSING, Event.Action.PUT, 2, "a", "b", "g");
        // Put succeeded any way
        applyAction(e, 2, Event.Action.GET, 2, "a", "b", "g");

        applyAction(e, 2, Event.Action.PUT, 3, "d");

        applyAction(e, null, Event.Action.CLEAR, null);
        applyAction(e, 0, Event.Action.SIZE, null);

        e.put("a", 1);
        applyAction(e, 1, Event.Action.SIZE, null, ".");
        applyAction(e, null, Event.Action.CLEAR, null, ".");
        applyAction(e, 0, Event.Action.SIZE, null, ".");
    }

    @Test
    void testAppend() {
        Event e = factory.newEvent();

        e.put("a", "1");
        applyAction(e, false, Event.Action.APPEND, "2", "a");

        e.put("a", null);
        applyAction(e, true, Event.Action.APPEND, "1", "a");
        Assertions.assertEquals(List.of("1"), e.get("a"));

        e.put("a", new ArrayList<>(List.of("1")));
        applyAction(e, true, Event.Action.APPEND, "2", "a");
        Assertions.assertEquals(List.of("1", "2"), e.get("a"));

        Assertions.assertThrows(IgnoredEventException.class,
                () -> applyAction(e, false, Event.Action.APPEND, NullOrMissingValue.MISSING, "a"));

        applyAction(e, true, Event.Action.APPEND, "1", "b");
        Assertions.assertEquals(List.of("1"), e.get("b"));

        applyAction(e, true, Event.Action.APPEND, null, "c");
        Assertions.assertEquals(List.of(NullOrMissingValue.NULL), e.get("c"));
    }

    @ParameterizedTest
    @MethodSource("appendArraySource")
    void testAppendArray(Object initial, Object toAppend, Object expected) {
        Event e = factory.newEvent();
        e.put("a", initial);
        applyAction(e, true, Event.Action.APPEND, toAppend, "a");
        Object result = e.get("a");
        if (expected instanceof float[]) {
            Assertions.assertArrayEquals((float[]) expected, (float[]) result, 1e-5f);
        } else if (expected instanceof double[]) {
            Assertions.assertArrayEquals((double[]) expected, (double[]) result, 1e-5);
        } else if (expected.getClass().isArray()) {
            Assertions.assertArrayEquals(new Object[]{expected}, new Object[]{result});
        } else {
            Assertions.assertEquals(expected, result);
        }
    }

    static Stream<Arguments> appendArraySource() {
        return Stream.of(
                Arguments.of(new char[]{'1'}, '2', new char[]{'1', '2'}),
                Arguments.of(new boolean[]{true}, false, new boolean[]{true, false}),
                Arguments.of(new byte[]{1}, 2, new byte[]{1, 2}),
                Arguments.of(new short[]{1}, 2, new short[]{1, 2}),
                Arguments.of(new int[]{1}, 2, new int[]{1, 2}),
                Arguments.of(new long[]{1L}, 2L, new long[]{1L, 2L}),
                Arguments.of(new float[]{1.0f}, 2.0f, new float[]{1.0f, 2.0f}),
                Arguments.of(new double[]{1.0}, 2.0, new double[]{1.0, 2.0}),
                Arguments.of(new String[]{"1"}, "2", new String[]{"1", "2"}),
                Arguments.of(new String[]{"1"}, null, new String[]{"1", null}),
                Arguments.of(new String[]{"1"}, NullOrMissingValue.NULL, new String[]{"1", null})
        );
    }

    @Test
    void testTimestamp() {
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        Assertions.assertEquals(new Date(0), e.applyAtPath(Event.Action.GET, VariablePath.TIMESTAMP, null));
        Assertions.assertEquals(new Date(0), e.applyAtPath(Event.Action.PUT, VariablePath.TIMESTAMP, new Date(1)));
        Assertions.assertEquals(new Date(1), e.applyAtPath(Event.Action.GET, VariablePath.TIMESTAMP, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> e.applyAtPath(Event.Action.PUT, VariablePath.TIMESTAMP, null));
    }

    @Test
    void testMeta() {
        Event e = factory.newEvent();
        e.putMeta("a", 1);
        Assertions.assertEquals(1, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("a"), null));
        Assertions.assertEquals(NullOrMissingValue.MISSING, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("b"), null));
        Assertions.assertEquals(NullOrMissingValue.MISSING, e.applyAtPath(Event.Action.PUT, VariablePath.ofMeta("b"), 2));
        Assertions.assertEquals(2, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("b"), null));
        Assertions.assertEquals(2, e.applyAtPath(Event.Action.REMOVE, VariablePath.ofMeta("b"), null));
        Assertions.assertNull(e.applyAtPath(Event.Action.CLEAR, VariablePath.ALLMETAS, null));
        Assertions.assertEquals(true, e.applyAtPath(Event.Action.ISEMPTY, VariablePath.ALLMETAS, null));
        Assertions.assertEquals(Set.of(),
                new HashSet<>((Collection<?>) e.applyAtPath(Event.Action.VALUES, VariablePath.ALLMETAS, null)));
        Assertions.assertEquals(Set.of(),
                new HashSet<>((Collection<?>) e.applyAtPath(Event.Action.KEYSET, VariablePath.ALLMETAS, null)));
    }

    private void applyAction(Event e, Object expected, Event.Action a, Object value, String... path) {
        Assertions.assertEquals(expected, e.applyAtPath(a, VariablePath.of(path), value), "Didn't resolve the path correctly");
    }

    @ParameterizedTest
    @MethodSource("applyAtPathSource")
    void testApplyAtPath(Event.Action action, VariablePath path, Object value, Object expected, String message) {
        Event ev;
        if (path.isContext()) {
            ev = factory.newEvent(MockConnectionContext.builder().properties(Map.of("prop", "val")).build());
        } else {
            ev = factory.newEvent();
        }
        ev.put("key", "value");
        ev.put("nested", new HashMap<>(Map.of("inner", "val")));
        ev.putMeta("mkey", "mval");

        Object result;
        try {
            result = ev.applyAtPath(action, path, value);
        } catch (Exception e) {
            result = e;
        }

        if (expected instanceof Class) {
            Assertions.assertInstanceOf((Class<?>) expected, result, message);
        } else if (result instanceof Set && expected instanceof Set) {
            Assertions.assertEquals(expected, result, message);
        } else if (result instanceof Collection && expected instanceof Set) {
            Assertions.assertEquals(expected, new HashSet<>((Collection<?>) result), message);
        } else if (result instanceof Collection && expected instanceof Collection) {
            Assertions.assertIterableEquals((Collection<?>) expected, (Collection<?>) result, message);
        } else {
            Assertions.assertEquals(expected, result, message);
        }
    }

    static Stream<Arguments> applyAtPathSource() {
        return Stream.of(
                Arguments.of(Event.Action.GET, VariablePath.parse("key"), null, "value", "GET failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("nested.inner"), null, "val", "GET nested failed"),
                Arguments.of(Event.Action.PUT, VariablePath.parse("newkey"), "newval", null, "PUT failed"),
                Arguments.of(Event.Action.CONTAINS, VariablePath.parse("key"), null, true, "CONTAINS failed"),
                Arguments.of(Event.Action.CONTAINS, VariablePath.parse("missing"), null, false, "CONTAINS missing failed"),
                Arguments.of(Event.Action.REMOVE, VariablePath.parse("key"), null, "value", "REMOVE failed"),
                Arguments.of(Event.Action.APPEND, VariablePath.parse("list"), "item1", true, "APPEND new failed"),
                Arguments.of(Event.Action.SIZE, VariablePath.parse("nested"), null, 1, "SIZE failed"),
                Arguments.of(Event.Action.ISEMPTY, VariablePath.parse("nested"), null, false, "ISEMPTY failed"),
                Arguments.of(Event.Action.KEYSET, VariablePath.parse("nested"), null, Set.of("inner"), "KEYSET failed"),
                Arguments.of(Event.Action.CONTAINSVALUE, VariablePath.parse("nested"), "val", true, "CONTAINSVALUE failed"),
                Arguments.of(Event.Action.VALUES, VariablePath.parse("nested"), null, List.of("val"), "VALUES failed"),
                Arguments.of(Event.Action.CLEAR, VariablePath.parse("nested"), null, null, "CLEAR failed"),
                Arguments.of(Event.Action.CHECK_WRAP, VariablePath.parse("nested"), "something", "something", "CHECK_WRAP success failed"),
                Arguments.of(Event.Action.ENTRYSET, VariablePath.parse("nested"), null, Set.of(Map.entry("inner", "val")), "ENTRYSET failed"),
                Arguments.of(Event.Action.GET, VariablePath.ofMeta("mkey"), null, "mval", "GET meta failed"),
                Arguments.of(Event.Action.GET, VariablePath.TIMESTAMP, null, java.util.Date.class, "GET timestamp failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("@context.remoteAddress"), null, "remote", "GET @context.remoteAddress failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("@context.localAddress"), null, "local", "GET @context.localAddress failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("@context.principal.name"), null, "user", "GET @context.principal.name failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("@context"), null, LockedConnectionContext.class, "GET @context failed"),
                Arguments.of(Event.Action.GET, VariablePath.parse("@context.prop"), null, "val", "GET @context property failed"),
                Arguments.of(Event.Action.KEYSET, VariablePath.parse("@context"), null, Set.of("prop", "localAddress", "remoteAddress", "principal"), "KEYSET @context failed"),
                Arguments.of(Event.Action.VALUES, VariablePath.parse("@context"), null, Set.of("val", "local", "remote", new TestLockedConnectionContext.MockPrincipal("user")), "VALUES @context failed"),
                Arguments.of(Event.Action.ENTRYSET, VariablePath.parse("@context"), null, Set.of(Map.entry("prop", "val"), Map.entry("localAddress", "local"), Map.entry("remoteAddress", "remote"), Map.entry("principal", new TestLockedConnectionContext.MockPrincipal("user"))), "ENTRYSET @context failed"),
                Arguments.of(Event.Action.SIZE, VariablePath.parse("@context"), null, 4, "SIZE @context failed"),
                Arguments.of(Event.Action.ISEMPTY, VariablePath.parse("@context"), null, false, "ISEMPTY @context failed"),
                Arguments.of(Event.Action.CONTAINSVALUE, VariablePath.parse("@context"), "local", true, "CONTAINSVALUE @context failed"),
                Arguments.of(Event.Action.CONTAINS, VariablePath.parse("@context.remoteAddress"), null, true, "CONTAINS @context should succeed"),
                Arguments.of(Event.Action.PUT, VariablePath.parse("@context.remoteAddress"), "new", IllegalArgumentException.class, "PUT @context should fail"),
                Arguments.of(Event.Action.APPEND, VariablePath.parse("@context.remoteAddress"), "new", IllegalArgumentException.class, "APPEND @context should fail"),
                Arguments.of(Event.Action.CONTAINS, VariablePath.ALLMETAS, null, false, "CONTAINS ALLMETAS failed"),
                Arguments.of(Event.Action.CONTAINSVALUE, VariablePath.ALLMETAS, "mval", true, "CONTAINSVALUE ALLMETAS failed"),
                Arguments.of(Event.Action.SIZE, VariablePath.ALLMETAS, null, 1, "SIZE ALLMETAS failed"),
                Arguments.of(Event.Action.ISEMPTY, VariablePath.ALLMETAS, null, false, "ISEMPTY ALLMETAS failed"),
                Arguments.of(Event.Action.GET, VariablePath.ALLMETAS, null, IllegalArgumentException.class, "GET ALLMETAS should fail"),
                Arguments.of(Event.Action.PUT, VariablePath.ALLMETAS, 1, IllegalArgumentException.class, "PUT ALLMETAS should fail"),
                Arguments.of(Event.Action.APPEND, VariablePath.ALLMETAS, 1, IllegalArgumentException.class, "APPEND ALLMETAS should fail"),
                Arguments.of(Event.Action.REMOVE, VariablePath.ALLMETAS, null, IllegalArgumentException.class, "REMOVE ALLMETAS should fail"),
                Arguments.of(Event.Action.APPEND, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "APPEND timestamp should fail"),
                Arguments.of(Event.Action.SIZE, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "SIZE timestamp should fail"),
                Arguments.of(Event.Action.CONTAINSVALUE, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "CONTAINSVALUE timestamp should fail"),
                Arguments.of(Event.Action.CONTAINS, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "CONTAINS timestamp should fail"),
                Arguments.of(Event.Action.REMOVE, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "REMOVE timestamp should fail"),
                Arguments.of(Event.Action.KEYSET, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "KEYSET timestamp should fail"),
                Arguments.of(Event.Action.VALUES, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "VALUES timestamp should fail"),
                Arguments.of(Event.Action.ISEMPTY, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "ISEMPTY timestamp should fail"),
                Arguments.of(Event.Action.CLEAR, VariablePath.TIMESTAMP, null, IllegalArgumentException.class, "CLEAR timestamp should fail")
        );
    }

}
