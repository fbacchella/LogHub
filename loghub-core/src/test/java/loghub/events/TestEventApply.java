package loghub.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.IgnoredEventException;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.VariablePath;

public class TestEventApply {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test
    public void testPath() {
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("a.b.c"), 1, true);
        e.put("d", 2);
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("e"), 3, true);
        e.applyAtPath(Event.Action.PUT, VariablePath.ofMeta("f"), 4, true);
        e.applyAtPath(Event.Action.PUT, VariablePath.parse("h"), Collections.emptyMap(), true);
        Assert.assertEquals(4, e.keySet().size());

        applyAction(e, true, Event.Action.CONTAINSVALUE, 2);
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1,"a");
        applyAction(e, true, Event.Action.CONTAINSVALUE, 1,"a", "b");
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1,"a", "b", "c");
        applyAction(e, false, Event.Action.CONTAINSVALUE, 1,"i");
        applyAction(e, true, Event.Action.CONTAINSVALUE, 2);

        applyAction(e, false, Event.Action.CONTAINS, null);
        applyAction(e, true, Event.Action.CONTAINS, null,"a");
        applyAction(e, true, Event.Action.CONTAINS, null,"a", "b");
        applyAction(e, true, Event.Action.CONTAINS, null,"a", "b", "c");
        applyAction(e, true, Event.Action.CONTAINS, null, "d");
        applyAction(e, false, Event.Action.CONTAINS, null,"d", "e");

        applyAction(e, 4, Event.Action.SIZE, null);
        applyAction(e, 1, Event.Action.SIZE, null, "a", "b");
        Assert.assertThrows(IgnoredEventException.class,
                () -> applyAction(e, 1, Event.Action.SIZE, null, "a", "b", "c"));
        Assert.assertThrows(IgnoredEventException.class,
                () -> applyAction(e, 0, Event.Action.SIZE, null, "i"));

        applyAction(e, 1, Event.Action.GET, null, "a", "b", "c");
        applyAction(e, NullOrMissingValue.MISSING, Event.Action.GET, null, "a", "b", "g");
        applyAction(e, 2, Event.Action.GET, null, "d");
        applyAction(e, 3, Event.Action.GET, null, "e");
        applyAction(e, NullOrMissingValue.MISSING, Event.Action.GET, null, "d", "e");
        Assert.assertEquals(4, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("f"), null));

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
    public void testAppend() {
        Event e = factory.newEvent();

        e.put("a", "1");
        applyAction(e, false, Event.Action.APPEND, "2", "a");

        e.put("a", null);
        applyAction(e, true, Event.Action.APPEND, "1", "a");
        Assert.assertEquals(List.of("1"), e.get("a"));

        e.put("a", new ArrayList<>(List.of("1")));
        applyAction(e, true, Event.Action.APPEND, "2", "a");
        Assert.assertEquals(List.of("1", "2"), e.get("a"));

        e.put("a", new char[]{'1'});
        applyAction(e, true, Event.Action.APPEND, '2', "a");
        Assert.assertArrayEquals(new char[]{'1', '2'}, (char[])e.get("a"));

        e.put("a", new boolean[]{true});
        applyAction(e, true, Event.Action.APPEND, false, "a");
        Assert.assertArrayEquals(new boolean[]{true, false}, (boolean[])e.get("a"));

        e.put("a", new byte[]{1});
        applyAction(e, true, Event.Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new byte[]{1, 2}, (byte[])e.get("a"));

        e.put("a", new short[]{1});
        applyAction(e, true, Event.Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new short[]{1, 2}, (short[])e.get("a"));

        e.put("a", new int[]{1});
        applyAction(e, true, Event.Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new int[]{1, 2}, (int[])e.get("a"));

        e.put("a", new long[]{1L});
        applyAction(e, true, Event.Action.APPEND, 2L, "a");
        Assert.assertArrayEquals(new long[]{1L, 2L}, (long[])e.get("a"));

        e.put("a", new float[]{1.0f});
        applyAction(e, true, Event.Action.APPEND, 2.0f, "a");
        Assert.assertArrayEquals(new float[]{1.0f, 2.0f}, (float[])e.get("a"), 1e-5f);

        e.put("a", new double[]{1.0});
        applyAction(e, true, Event.Action.APPEND, 2.0, "a");
        Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[])e.get("a"), 1e-5);

        e.put("a", new String[]{"1"});
        applyAction(e, true, Event.Action.APPEND, "2", "a");
        Assert.assertArrayEquals(new String[]{"1", "2"}, (String[])e.get("a"));

        e.put("a", new String[]{"1"});
        applyAction(e, true, Event.Action.APPEND, null, "a");
        Assert.assertArrayEquals(new String[]{"1", null}, (String[])e.get("a"));

        e.put("a", new String[]{"1"});
        applyAction(e, true, Event.Action.APPEND, NullOrMissingValue.NULL, "a");
        Assert.assertArrayEquals(new String[]{"1", null}, (String[])e.get("a"));

        Assert.assertThrows(IgnoredEventException.class,
                () -> applyAction(e, false, Event.Action.APPEND, NullOrMissingValue.MISSING, "a"));

        applyAction(e, true, Event.Action.APPEND, "1", "b");
        Assert.assertEquals(List.of("1"), e.get("b"));

        applyAction(e, true, Event.Action.APPEND, null, "c");
        Assert.assertEquals(List.of(NullOrMissingValue.NULL), e.get("c"));
    }

    @Test
    public void testTimestamp() {
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        Assert.assertEquals(new Date(0), e.applyAtPath(Event.Action.GET, VariablePath.TIMESTAMP, null));
        Assert.assertEquals(new Date(0), e.applyAtPath(Event.Action.PUT, VariablePath.TIMESTAMP, new Date(1)));
        Assert.assertEquals(new Date(1), e.applyAtPath(Event.Action.GET, VariablePath.TIMESTAMP, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> e.applyAtPath(Event.Action.PUT, VariablePath.TIMESTAMP, null));
        for (Event.Action a: List.of(Event.Action.APPEND,
                                    Event.Action.SIZE,
                                    Event.Action.CONTAINSVALUE,
                                    Event.Action.CONTAINS,
                                    Event.Action.SIZE,
                                    Event.Action.REMOVE,
                                    Event.Action.KEYSET,
                                    Event.Action.VALUES,
                                    Event.Action.ISEMPTY,
                                    Event.Action.CLEAR
                )
        ) {
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> e.applyAtPath(a, VariablePath.TIMESTAMP, null));
        }
    }

    @Test
    public void testMeta() {
        Event e = factory.newEvent();
        e.putMeta("a", 1);
        Assert.assertEquals(false, e.applyAtPath(Event.Action.CONTAINS, VariablePath.ALLMETAS, null));
        Assert.assertEquals(false, e.applyAtPath(Event.Action.CONTAINS, VariablePath.ALLMETAS, "b"));
        Assert.assertEquals(true, e.applyAtPath(Event.Action.CONTAINS, VariablePath.ofMeta("a"), null));
        Assert.assertEquals(false, e.applyAtPath(Event.Action.CONTAINS, VariablePath.ofMeta("b"), null));
        Assert.assertEquals(true, e.applyAtPath(Event.Action.CONTAINSVALUE, VariablePath.ALLMETAS, 1));
        Assert.assertEquals(false, e.applyAtPath(Event.Action.CONTAINSVALUE, VariablePath.ALLMETAS, 2));
        Assert.assertEquals(1, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("a"), null));
        Assert.assertEquals(1, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("a"), null));
        Assert.assertEquals(NullOrMissingValue.MISSING, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("b"), null));
        Assert.assertEquals(NullOrMissingValue.MISSING, e.applyAtPath(Event.Action.PUT, VariablePath.ofMeta("b"), 2));
        Assert.assertEquals(2, e.applyAtPath(Event.Action.GET, VariablePath.ofMeta("b"), null));
        Assert.assertEquals(2, e.applyAtPath(Event.Action.REMOVE, VariablePath.ofMeta("b"), null));
        Assert.assertEquals(1, e.applyAtPath(Event.Action.SIZE, VariablePath.ALLMETAS, null));
        Assert.assertEquals(false, e.applyAtPath(Event.Action.ISEMPTY, VariablePath.ALLMETAS, null));
        Assert.assertNull(e.applyAtPath(Event.Action.CLEAR, VariablePath.ALLMETAS, null));
        Assert.assertEquals(true, e.applyAtPath(Event.Action.ISEMPTY, VariablePath.ALLMETAS, null));
        Assert.assertEquals(Collections.emptySet(),
                new HashSet<>( (Collection) e.applyAtPath(Event.Action.VALUES, VariablePath.ALLMETAS, null)));
        Assert.assertEquals(Collections.emptySet(),
                new HashSet<>( (Collection) e.applyAtPath(Event.Action.KEYSET, VariablePath.ALLMETAS, null)));
        for(Event.Action a: List.of(Event.Action.GET,
                                    Event.Action.PUT,
                                    Event.Action.APPEND,
                                    Event.Action.REMOVE
        )) {
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> e.applyAtPath(a, VariablePath.ALLMETAS, null));
        }
    }

    private void applyAction(Event e, Object expected, Event.Action a, Object value, String... path) {
        Assert.assertEquals("Didn't resolve the path correctly",  expected, e.applyAtPath(a, VariablePath.of(path), value));
    }

}
