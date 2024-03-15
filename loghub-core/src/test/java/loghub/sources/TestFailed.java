package loghub.sources;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Test;

import loghub.ProcessorException;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;

public class TestFailed {

    @Test(expected=ConfigException.class)
    public void testLog() throws ConfigException, IOException, ProcessorException {
        String confile = "pipeline[main] {[a] @ [a] %source1}";
        Configuration.parse(new StringReader(confile));
    }

}
