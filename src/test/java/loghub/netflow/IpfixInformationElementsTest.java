package loghub.netflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.netflow.IpfixInformationElements;

public class IpfixInformationElementsTest {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void test() throws IOException {
        IpfixInformationElements iie = new IpfixInformationElements();
        Map<String, Set<String>> values = new HashMap<>();
        iie.elements.forEach((k,v) -> {
            values.computeIfAbsent("Abstract Data Type", (i) -> new HashSet<>()).add(v.type);
            values.computeIfAbsent("Data Type Semantics", (i) -> new HashSet<>()).add(v.semantics);
            values.computeIfAbsent("Status", (i) -> new HashSet<>()).add(v.semantics);
            values.computeIfAbsent("Units", (i) -> new HashSet<>()).add(v.units);
            values.computeIfAbsent("Requester", (i) -> new HashSet<>()).add(v.requester);
        });
        Assert.assertNotEquals(0, values.size());
        values.forEach((i, j) -> {
            logger.debug("{} {}", i, j);
            Assert.assertNotEquals(0, j.size());
        });
    }
}
