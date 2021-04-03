package loghub.processors;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestScanBinary {

    @Test
    public void simpleTestWithNames() throws ProcessorException {
        ScanBinary fs = new ScanBinary();
        fs.setBitsNames(new String[] {"PF_PROT", "PF_WRITE", "PF_USER", "PF_RSVD", "PF_INSTR"});
        fs.configure(new Properties(Collections.emptyMap()));
        fs.setField(VariablePath.of(new String[] {"binary"}));

        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.put("binary", "13");
        Assert.assertTrue(fs.process(e));
        String[] processed = (String[]) e.get("binary");
        Assert.assertArrayEquals("Bad decoding of bitfield", new String[] {"PF_PROT", "PF_USER", "PF_RSVD"}, processed);
    }

    @Test
    public void simpleTestWithVariableLengthNames() throws ProcessorException {
        ScanBinary fs = new ScanBinary();
        fs.setBitsNames(new String[] {"a", "b", "c"});
        fs.setAsMap(true);
        fs.configure(new Properties(Collections.emptyMap()));
        fs.setField(VariablePath.of(new String[] {"binary"}));

        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.put("binary", 0b101);
        Assert.assertTrue(fs.process(e));
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) fs.fieldFunction(e, 0b101);
        Assert.assertEquals(1, value.get("a").intValue());
        Assert.assertEquals(0, value.get("b").intValue());
        Assert.assertEquals(1, value.get("c").intValue());
    }

    @Test
    public void simpleTestWithVariableLengthName2s() throws ProcessorException {
        ScanBinary fs = new ScanBinary();
        fs.setBitsNames(new String[] {"a", "b", "c"});
        fs.setFieldsLength(new Integer[] {3, 2, 1});
        fs.configure(new Properties(Collections.emptyMap()));
        fs.setField(VariablePath.of(new String[] {"binary"}));
        fs.setDestination("value");

        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.put("binary", 0b110101);
        Assert.assertTrue(fs.process(e));
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) e.get("value");
        Assert.assertEquals(0b101, value.get("a").intValue());
        Assert.assertEquals(0b10, value.get("b").intValue());
        Assert.assertEquals(0b1, value.get("c").intValue());
    }

    @Test
    public void simpleTestNoName() throws ProcessorException {
        ScanBinary fs = new ScanBinary();
        Assert.assertFalse(fs.configure(new Properties(Collections.emptyMap())));
    }

    @Test
    public void testConfigFile() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("scanbinary.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = Tools.getEvent();
        sent.put("binary", 0b110101);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        @SuppressWarnings("unchecked")
        Map<String, Number> value = (Map<String, Number>) sent.get("binary");
        Assert.assertEquals(0b101, value.get("a").intValue());
        Assert.assertEquals(0b10, value.get("b").intValue());
        Assert.assertEquals(0b1, value.get("c").intValue());
    }

}
