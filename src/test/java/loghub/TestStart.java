package loghub;

import java.io.IOException;

import org.junit.Test;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;

public class TestStart {

    @Test(timeout=5000)
    public void runStart() throws ConfigException, IOException {
        String conffile = Configuration.class.getClassLoader().getResource("test.conf").getFile();
        Start.main(new String[] {"-T", "-c", conffile});
    }
}
