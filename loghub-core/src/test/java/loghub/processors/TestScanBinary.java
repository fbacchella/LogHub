package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import loghub.Helpers;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestScanBinary {

    private final EventsFactory factory = new EventsFactory();

    @Test
    void simpleTestWithNames() throws ProcessorException {
        ScanBinary.Builder builder = ScanBinary.getBuilder();
        builder.setBitsNames(new String[] {"PF_PROT", "PF_WRITE", "PF_USER", "PF_RSVD", "PF_INSTR"});
        builder.setField(VariablePath.of("binary"));
        ScanBinary fs = builder.build();
        fs.configure(new Properties(Collections.emptyMap()));

        Event e = factory.newEvent();
        e.put("binary", "13");
        Assertions.assertTrue(fs.process(e));
        String[] processed = (String[]) e.get("binary");
        Assertions.assertArrayEquals(new String[] {"PF_PROT", "PF_USER", "PF_RSVD"}, processed, "Bad decoding of bitfield");
    }

    @Test
    void simpleTestWithVariableLengthNames() throws ProcessorException {
        ScanBinary.Builder builder = ScanBinary.getBuilder();
        builder.setBitsNames(new String[] {"a", "b", "c"});
        builder.setAsMap(true);
        builder.setField(VariablePath.of("binary"));
        ScanBinary fs = builder.build();
        fs.configure(new Properties(Collections.emptyMap()));

        Event e = factory.newEvent();
        e.put("binary", 0b101);
        Assertions.assertTrue(fs.process(e));
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) fs.fieldFunction(e, 0b101);
        Assertions.assertEquals(1, value.get("a").intValue());
        Assertions.assertEquals(0, value.get("b").intValue());
        Assertions.assertEquals(1, value.get("c").intValue());
    }

    @Test
    void simpleTestWithVariableLengthName2s() throws ProcessorException {
        ScanBinary.Builder builder = ScanBinary.getBuilder();
        builder.setBitsNames(new String[] {"a", "b", "c"});
        builder.setFieldsLength(new Integer[] {3, 2, 1});
        builder.setField(VariablePath.of("binary"));
        builder.setDestination(VariablePath.parse("value"));
        ScanBinary fs = builder.build();
        fs.configure(new Properties(Collections.emptyMap()));

        Event e = factory.newEvent();
        e.put("binary", 0b110101);
        Assertions.assertTrue(fs.process(e));
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) e.get("value");
        Assertions.assertEquals(0b101, value.get("a").intValue());
        Assertions.assertEquals(0b10, value.get("b").intValue());
        Assertions.assertEquals(0b1, value.get("c").intValue());
    }

    @Test
    void simpleTestNoName() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> ScanBinary.getBuilder().build());
        Assertions.assertEquals("Missing mandatory attribute bitsNames", ex.getMessage());
    }

    @Test
    void testConfigFile() throws ConfigException, IOException {
        String confSource = """
            pipeline[main] {
                loghub.processors.ScanBinary {
                    fieldsLength: [3, 2, 1],
                    bitsNames: ["a", "b", "c"],
                    field: "binary"
                }
            }
        """;
        Properties conf = Configuration.parse(new StringReader(confSource));
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("binary", 0b110101);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) sent.get("binary");
        Assertions.assertEquals(0b101, value.get("a").intValue());
        Assertions.assertEquals(0b10, value.get("b").intValue());
        Assertions.assertEquals(0b1, value.get("c").intValue());
    }

}
