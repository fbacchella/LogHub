package loghub;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class PathTreeTest {

    @Test
    public void test() {
        PathTree<String, VariablePath> tree = new PathTree(VariablePath.of(""));
        for (String[] i: List.of(new String[] {"a", "b"}, new String[] {"a", "b" , "c"}, new String[] {"a", "d"}, new String[] {"a", "e", "f"})) {
            VariablePath vp1 = tree.computeIfAbsent(i, () -> VariablePath.of(i));
            VariablePath vp2 = tree.computeIfAbsent(i, () -> VariablePath.of(i));
            Assert.assertSame(vp1, vp2);
            Assert.assertSame(vp1, tree.findByPath(i));
            Assert.assertEquals(String.format("[%s]", String.join(".", i)), tree.findByPath(i).toString());
        }

        String[] parent = new String[] {"a", "g", "h"};
        VariablePath vp1 = tree.computeChildIfAbsent(parent, "i", () -> VariablePath.of(new String[] {"a", "g", "h", "i"}));
        VariablePath vp2 = tree.computeChildIfAbsent(parent, "i", () -> VariablePath.of(new String[] {"a", "g", "h", "i"}));
        Assert.assertSame(vp1, vp2);
        Assert.assertEquals("[a.g.h.i]", vp1.toString());

        // Resolve a child before the parent
        VariablePath vp3 = tree.computeChildIfAbsent(new String[]{"j"}, "k", () -> VariablePath.of(new String[] {"j", "k"}));
        Assert.assertEquals("[j.k]", vp3.toString());

        VariablePath vp4 = tree.computeChildIfAbsent(new String[]{}, "j", () -> VariablePath.of(new String[] {"j"}));
        Assert.assertEquals("[j]", vp4.toString());
    }

}
