package loghub.decoders.netflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class IpfixInformationElementsTest {

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
        values.forEach((i, j) -> {
            System.out.format("%s %s\n", i, j);
        });
    }
}
