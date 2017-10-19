package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class TestPrintBinary {

    @Test
    public void simpleTestWitName() throws ProcessorException {
        PrintBinary fs = new PrintBinary();
        fs.setBitsNames(new String[] {"PF_PROT", "PF_WRITE", "PF_USER", "PF_RSVD", "PF_INSTR"});
        fs.configure(new Properties(Collections.emptyMap()));

        Event e = Event.emptyEvent();
        e.put("binary", "14");
        fs.processMessage(e, "binary", "value");
        Assert.assertArrayEquals("Bad decoding of bitfield", new String[] {"PF_WRITE", "PF_USER", "PF_RSVD"}, (String[])e.get("value"));
    }

    @Test
    public void simpleTestNoName() throws ProcessorException {
        PrintBinary fs = new PrintBinary();
        fs.configure(new Properties(Collections.emptyMap()));

        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.put("binary", "14");
        fs.processMessage(e, "binary", "value");
        Assert.assertEquals("Bad decoding of bitfield", "0b1110", e.get("value"));
    }
}
