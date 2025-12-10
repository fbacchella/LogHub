package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.Processor;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestFlatten {

    private final EventsFactory factory = new EventsFactory();
    private static final Logger logger = LogManager.getLogger();

    /* ====================================================================== */
    /*  Simple values                                                         */
    /* ====================================================================== */

    @Test
    @DisplayName("Simple standalone value must remain unchanged")
    void testSimpleValue() {
        Assertions.assertEquals("hello", Expression.flatten("hello"));
    }

    /* ====================================================================== */
    /*  Missing or null values                                                */
    /* ====================================================================== */

    static Stream<Arguments> provideNull() {
        return Stream.of(
                Arguments.of(
                        null,
                        NullOrMissingValue.NULL
                ),
                Arguments.of(
                        List.of(NullOrMissingValue.NULL),
                        NullOrMissingValue.NULL
                ),
                Arguments.of(
                        Collections.singletonList(null),
                        NullOrMissingValue.NULL
                ),
                Arguments.of(
                        new Object[]{null, NullOrMissingValue.NULL},
                        List.of(NullOrMissingValue.NULL, NullOrMissingValue.NULL)
                ),
                Arguments.of(
                        List.of(NullOrMissingValue.MISSING),
                        List.of()
                ),
                Arguments.of(
                        List.of(NullOrMissingValue.MISSING, 1),
                        1
                ),
                Arguments.of(
                        List.of(NullOrMissingValue.MISSING, 1, 2),
                        List.of(1, 2)
                ),
                Arguments.of(
                        NullOrMissingValue.NULL,
                        NullOrMissingValue.NULL
                )
        );
    }

    @DisplayName("Null or missing must be correctly filtered")
    @ParameterizedTest(name = "Flatten {0}")
    @MethodSource("provideNull")
    void testNullValue(Object input, Object expected) {
        Assertions.assertEquals(expected, Expression.flatten(input));
    }

    @Test
    @DisplayName("Ignored missing value")
    void testMissing() {
        Assertions.assertThrows(IgnoredEventException.class, () -> Expression.flatten(NullOrMissingValue.MISSING));
    }

    /* ====================================================================== */
    /*  Collections                                                           */
    /* ====================================================================== */

    @Test
    @DisplayName("Simple collection must remain unchanged")
    void testSimpleCollection() {
        List<Integer> src = List.of(1, 2, 3);
        Assertions.assertEquals(List.of(1, 2, 3), Expression.flatten(src));
    }

    @Test
    @DisplayName("Nested collection must be flattened")
    void testNestedCollection() {
        List<Object> src = Arrays.asList(1, Arrays.asList(2, 3));
        Assertions.assertEquals(List.of(1, 2, 3), Expression.flatten(src));
    }

    @Test
    @DisplayName("Set must remain a Set and be flattened")
    void testSetFlattening() {
        Set<Object> src = Set.of(1, Set.of(2, 3), new int[]{4, 5});
        Object r = Expression.flatten(src);

        Assertions.assertTrue(r instanceof Set<?>);
        Assertions.assertEquals(Set.of(1, 2, 3, 4, 5), r);
    }

    /* ====================================================================== */
    /*  Streams                                                               */
    /* ====================================================================== */

    @Test
    @DisplayName("Stream containing nested structures must be flattened")
    void testStreamFlattening() {
        Stream<?> src = Stream.of(1, Stream.of(2, 3), Arrays.asList(4, 5));
        Collection<?> c = (Collection<?>) Expression.flatten(src);
        Assertions.assertEquals(List.of(1, 2, 3, 4, 5), c);
    }

    @Test
    @DisplayName("Nested streams must be flattened")
    void testNestedStream() {
        Stream<?> v = Stream.of(1, Stream.of(2, Stream.of(3, 4)), 5);
        Collection<?> c = (Collection<?>) Expression.flatten(v);
        Assertions.assertEquals(List.of(1, 2, 3, 4, 5), c);
    }

    @Test
    @DisplayName("Stream must be consumed")
    void testStreamConsumption() {
        Stream<Integer> s = Stream.of(1, 2, 3);
        Expression.flatten(s);
        Assertions.assertThrows(IllegalStateException.class, s::count);
    }

    @Test
    @DisplayName("Detect consumed stream")
    void testStreamConsumed() {
        Stream<Integer> s = Stream.of(1, 2, 3);
        s.count();
        Assertions.assertThrows(IllegalStateException.class, () -> Expression.flatten(s));
    }

    /* ====================================================================== */
    /*  Arrays (generic + primitive), with parametrers                        */
    /* ====================================================================== */

    static Stream<Arguments> provideObjectArrays() {
        return Stream.of(
                Arguments.of(
                        new Object[]{"a", new Object[]{"b", "c"}},
                        List.of("a", "b", "c")
                ),
                Arguments.of(
                        new Object[]{1, new int[]{2, 3}, 4},
                        List.of(1, 2, 3, 4)
                ),
                Arguments.of(
                        new Object[]{1, new Object[]{2, new Object[]{3, 4}}, 5},
                        List.of(1, 2, 3, 4, 5)
                )
        );
    }

    @ParameterizedTest(name = "Flatten object array #{index}")
    @MethodSource("provideObjectArrays")
    @DisplayName("Object arrays must be correctly flattened")
    void testObjectArrays(Object[] input, List<?> expected) {
        Assertions.assertEquals(expected, Expression.flatten(input));
    }

    static Stream<Arguments> providePrimitiveArrays() {
        return Stream.of(
                Arguments.of(new int[]{1, 2, 3}, List.of(1, 2, 3)),
                Arguments.of(new long[]{1L, 2L}, List.of(1L, 2L)),
                Arguments.of(new float[]{1f, 2f}, List.of(1f, 2f)),
                Arguments.of(new double[]{1.0, 2.0}, List.of(1.0, 2.0)),
                Arguments.of(new byte[]{10, 20}, List.of((byte) 10, (byte) 20)),
                Arguments.of(new short[]{1, 2}, List.of((short) 1, (short) 2)),
                Arguments.of(new char[]{'a', 'b'}, List.of('a', 'b')),
                Arguments.of(new boolean[]{true, false}, List.of(true, false))
        );
    }

    @ParameterizedTest(name = "Flatten primitive array #{index}")
    @MethodSource("providePrimitiveArrays")
    @DisplayName("Primitive arrays must be flattened")
    void testPrimitiveArrays(Object array, List<?> expected) {
        Assertions.assertEquals(expected, Expression.flatten(array));
    }

    /* ====================================================================== */
    /*  Mixed and deep structures                                             */
    /* ====================================================================== */

    @Test
    @DisplayName("Mixed complex structures must be flattened")
    void testMixed() {
        Object[] v = {
                1,
                List.of(2, new int[]{3, 4}),
                Stream.of(5, new Object[]{6, 7}),
                new boolean[]{true, false}
        };
        Object r = Expression.flatten(v);

        Assertions.assertEquals(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, true, false),
                r
        );
    }

    @Test
    @DisplayName("Null inside a structure must become NullOrMissingValue.NULL")
    void testNullInsideStructure() {
        List<Object> src = Arrays.asList(1, null, 3);
        Object r = Expression.flatten(src);

        Assertions.assertEquals(
                List.of(1, NullOrMissingValue.NULL, 3),
                r
        );
    }

    /* ====================================================================== */
    /*  Unwrapped single entry                                                */
    /* ====================================================================== */

    static Stream<Arguments> provideSingle() {
        return Stream.of(
                // Primitive arrays with a single element
                Arguments.of(new int[]{1}, 1),
                Arguments.of(new long[]{42L}, 42L),
                Arguments.of(new double[]{3.14}, 3.14),
                Arguments.of(new byte[]{127}, (byte) 127),
                Arguments.of(new short[]{100}, (short) 100),
                Arguments.of(new float[]{2.71f}, 2.71f),
                Arguments.of(new char[]{'a'}, 'a'),
                Arguments.of(new boolean[]{true}, true),

                // Object arrays with a single element
                Arguments.of(new Integer[]{5}, 5),
                Arguments.of(new String[]{"hello"}, "hello"),
                Arguments.of(new Object[]{42}, 42),

                // Collections with a single element
                Arguments.of(List.of(10), 10),
                Arguments.of(Set.of("single"), "single"),
                Arguments.of(Collections.singletonList(99), 99),
                Arguments.of(Collections.singletonList(Arrays.asList(1, 2, 3)), List.of(1, 2, 3)),

                // Nested singletons - double-wrapped
                Arguments.of(List.of(new int[]{42}), 42),
                Arguments.of(new Object[]{List.of(7)}, 7),
                Arguments.of(List.of(Set.of(15)), 15),

                // Triple nested singletons
                Arguments.of(List.of(List.of(List.of(3))), 3),
                Arguments.of(List.of(Arrays.asList(new Object[]{99})), 99),

                // Nested collections with flattening
                Arguments.of(Set.of(List.of(5)), 5),

                // Single element wrapped multiple times
                Arguments.of(
                        List.of(Collections.singletonList(Arrays.asList(new int[]{8}))),
                        8
                ),

                //Stream is unwrapped
                Arguments.of(
                        Stream.of(8),
                        8
                ),

                //Empty elements are discared
                Arguments.of(
                        List.of(List.of(), List.of(8)),
                        8
                ),
                Arguments.of(
                        List.of(List.of(8), List.of()),
                        8
                ),
                Arguments.of(
                        List.of(new int[0], List.of(8)),
                        8
                )

        );
    }

    @ParameterizedTest(name = "Flatten singleton #{index}: {0} => {1}")
    @MethodSource("provideSingle")
    @DisplayName("Singleton structures must be unwrapped to single value")
    void testUnwrap(Object input, Object expected) {
        Object result = Expression.flatten(input);
        Assertions.assertEquals(expected, result);
    }


    /* ====================================================================== */
    /*  Empty structures                                                      */
    /* ====================================================================== */

    @Test
    @DisplayName("Empty collection must return empty list")
    void testEmptyCollection() {
        Object r = Expression.flatten(List.of());
        Assertions.assertEquals(List.of(), r);
    }

    @Test
    @DisplayName("Empty array must return empty list")
    void testEmptyObjectArray() {
        Object r = Expression.flatten(new Object[0]);
        Assertions.assertEquals(List.of(), r);
    }

    @Test
    @DisplayName("Empty stream must return empty stream")
    void testEmptyStream() {
        Collection<?> c = (Collection<?>) Expression.flatten(Stream.empty());
        Assertions.assertEquals(0, c.size());
    }

    @DisplayName("The iterate attribute is ignored")
    @Test
    void testIterateAttribute() {
        Flatten.Builder builder = Flatten.getBuilder();
        builder.setIterate(true);
        Flatten flatten = builder.build();
        Event ev = factory.newTestEvent();
        ev.putAtPath(VariablePath.of("message"), Set.of(List.of(1), Set.of(1)));
        Tools.runProcessing(ev, "main", List.of(flatten));
        Integer result = (Integer) ev.getAtPath(VariablePath.of("message"));
        Assertions.assertEquals(1, result);
    }

    @Test
    @DisplayName("Check processor attributes")
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Flatten"
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
