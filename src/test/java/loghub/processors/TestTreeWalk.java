package loghub.processors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import lombok.Setter;

/**
 * The tests check the tree traversal by comparing with results from <a href="https://en.wikipedia.org/wiki/Tree_traversal">...</a>
 */
public class TestTreeWalk {

    private static class TreeWalkTester extends TreeWalkProcessor {
        public static class Builder extends TreeWalkProcessor.Builder<TreeWalkTester> {
            @Setter
            private boolean saveNodes = false;
            public TreeWalkTester build() {
                return new TreeWalkTester(this);
            }
        }
        public static TreeWalkTester.Builder getBuilder() {
            return new TreeWalkTester.Builder();
        }

        private final List<Object> leafs = new ArrayList<>();
        private final boolean saveNodes;

        TreeWalkTester(Builder builder) {
            super(builder);
            saveNodes = builder.saveNodes;
        }

        @Override
        protected Object processLeaf(Event event, Object value) {
            leafs.add(value);
            return RUNSTATUS.NOSTORE;
        }

        @Override
        protected Object processNode(Event event, Map<String, Object> value) {
            if (saveNodes) {
                if (value.containsKey("b")) {
                    leafs.add("f");
                } else if (value.containsKey("a")) {
                    leafs.add("b");
                } else if (value.containsKey("c")) {
                    leafs.add("d");
                } else if (value.containsKey("i")) {
                    leafs.add("g");
                } else if (value.containsKey("h")) {
                    leafs.add("i");
                }
            }
            return RUNSTATUS.NOSTORE;
        }
    }

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
    }

    private Event getWikipediaEvent() {
        // Build manually the event to ensure deterministic order of nodes.
        Event ev = factory.newEvent();
        Map<String, Object> f = new LinkedHashMap<>();
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, Object> b = new LinkedHashMap<>();
        Map<String, Object> g = new LinkedHashMap<>();
        Map<String, Object> i = new LinkedHashMap<>();
        ev.put("f", f);
        f.put("b", b);
        f.put("g", g);
        b.put("a", "a");
        b.put("d", d);
        d.put("c", "c");
        d.put("e", "e");
        g.put("i", i);
        i.put("h", "h");
        return ev;
    }

    private void runTest(Consumer<TreeWalkTester.Builder> conf, Event e, List<Object> expected) throws ProcessorException {
        TreeWalkTester.Builder builder = TreeWalkTester.getBuilder();
        builder.setField(VariablePath.of("f"));
        conf.accept(builder);
        TreeWalkTester w = builder.build();
        Tools.runProcessing(e, "main", List.of(w));
        Assert.assertEquals(expected, w.leafs);
    }

    private void runTest(Consumer<TreeWalkTester.Builder> conf, List<Object> expected) throws ProcessorException {
        runTest(conf, getWikipediaEvent(), expected);
    }

    @Test
    public void testDefaultWikipedia() throws ProcessorException {
        runTest(b -> b.setSaveNodes(true), List.of("f", "b", "g", "a", "d", "i", "c", "e", "h"));
    }

    @Test
    public void testDepthWikipedia() throws ProcessorException {
        Event e = factory.newEvent();
        e.putAtPath(VariablePath.parse("f.b.a"), "a");
        e.putAtPath(VariablePath.parse("f.b.d.c"), "c");
        e.putAtPath(VariablePath.parse("f.b.d.e"), "e");
        e.putAtPath(VariablePath.parse("f.g.i.h"), "h");
        runTest(b -> {b.setTraversal(FieldsProcessor.TRAVERSAL_ORDER.DEPTH); b.setSaveNodes(true);}, List.of("a", "c", "e", "d", "b", "h", "i", "g", "f"));
    }

    @Test
    public void testBreadthWikipedia() throws ProcessorException {
        runTest(b -> {b.setTraversal(FieldsProcessor.TRAVERSAL_ORDER.BREADTH); b.setSaveNodes(true);}, List.of("f", "b", "g", "a", "d", "i", "c", "e", "h"));
    }

    @Test
    public void testIterable() throws ProcessorException {
        Event e = factory.newEvent();
        e.putAtPath(VariablePath.parse("f.b.c"), List.of("1", "2"));
        e.putAtPath(VariablePath.parse("f.b.d"), "3");
        e.putAtPath(VariablePath.parse("f.e"), "4");
        runTest(b -> b.setTraversal(FieldsProcessor.TRAVERSAL_ORDER.DEPTH), e, List.of(List.of("1", "2"), "3", "4"));
    }

}
