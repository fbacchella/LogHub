package loghub;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestVariablePath {
    
    @Test
    public void single() {
        List<String> splited = VariablePath.pathElements("a");
        Assert.assertArrayEquals(new String[] {"a"}, splited.stream().toArray(String[]::new));
    }

    @Test
    public void simple() {
        List<String> splited = VariablePath.pathElements("a.b.c");
        Assert.assertArrayEquals(new String[] {"a", "b", "c"}, splited.stream().toArray(String[]::new));
    }

    @Test
    public void rooted() {
        List<String> splited = VariablePath.pathElements(".a.b.c");
        Assert.assertArrayEquals(new String[] {".", "a", "b", "c"}, splited.stream().toArray(String[]::new));
    }

    @Test
    public void dup() {
        List<String> splited = VariablePath.pathElements("a..b..c.");
        Assert.assertArrayEquals(new String[] {"a", "b", "c"}, splited.stream().toArray(String[]::new));
    }

    @Test
    public void dirty() {
        List<String> splited = VariablePath.pathElements("a..b..c..");
        Assert.assertArrayEquals(new String[] {"a", "b", "c"}, splited.stream().toArray(String[]::new));
    }

}
