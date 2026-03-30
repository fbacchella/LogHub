package loghub.configuration;

import org.junit.Assert;
import org.junit.Test;

public class TestCharSupport {

    @Test
    public void testSimple() {
        Assert.assertEquals("\1", CharSupport.getStringFromGrammarStringLiteral("\"\\01\""));
        Assert.assertEquals("1\1", CharSupport.getStringFromGrammarStringLiteral("\"1\\1\""));
        Assert.assertEquals("loghub", CharSupport.getStringFromGrammarStringLiteral("\"loghub\""));
        Assert.assertEquals("\1", CharSupport.getStringFromGrammarStringLiteral("\"\\u0001\""));
        Assert.assertEquals("α", CharSupport.getStringFromGrammarStringLiteral("\"\\u03B1\""));
        Assert.assertEquals("Aαa", CharSupport.getStringFromGrammarStringLiteral("\"A\\u03B1a\""));
        Assert.assertEquals("\n", CharSupport.getStringFromGrammarStringLiteral("\"\\n\""));
        Assert.assertEquals("\\n", CharSupport.getStringFromGrammarStringLiteral("\"\\\\n\""));
    }

}
