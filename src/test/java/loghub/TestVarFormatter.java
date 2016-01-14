package loghub;

import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestVarFormatter {

    private void checkFormat(Object value, String format) {
        Map<String, Object> values = Collections.singletonMap("var", value);
        for(Locale l: Locale.getAvailableLocales()) {
            VarFormatter vf = new VarFormatter("${var" + format + "}", l);
            String printf = String.format(l, format, value);
            String formatter = vf.format(values);
            Assert.assertEquals("mismatch for " + format, printf, formatter );
        }
    }
    
    @Test
    public void test1() {
        checkFormat(65535, "%#X");
        checkFormat(65535, "%#x");
    }

    @Test
    public void test2() {
        checkFormat(65535, "%#o");
        // unknown to format checkFormat(65535, "%#O");
    }

    @Test
    public void test3() {
        checkFormat(65535, "%-10d");
        checkFormat(65535, "%d");
        checkFormat(-65535, "%(d");
    }

    @Test
    public void test4() {
        checkFormat(Math.PI, "%f");
        checkFormat(Math.PI, "%10.2f");
        checkFormat(Math.PI, "%+10.2f");
    }

    @Test
    public void test5() {
        Date epoch = new Date(0);
        Map<String, Object> values = Collections.singletonMap("var", epoch);
        VarFormatter vf = new VarFormatter("${var%t<Europe/Paris>H}", Locale.US);
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for time zone parsing" , "01", formatter );
        checkFormat(new Date(), "%tH");
        checkFormat(new Date(), "%tm");
        checkFormat(new Date(), "%tj");
        checkFormat(new Date(), "%ta");
        checkFormat(new Date(), "%tb");
        checkFormat(new Date(), "%tp");
    }

    @Test
    public void testLocale() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        VarFormatter vf = new VarFormatter("${var%d:" + Locale.CHINA.toLanguageTag() + "}");
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for time zone parsing" , "1", formatter );
    }

}
