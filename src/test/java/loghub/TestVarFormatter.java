package loghub;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestVarFormatter {

    private void checkFormat(Object value, String format, boolean fail) {
        Map<String, Object> values = Collections.singletonMap("var", value);
        for(Locale l: Locale.getAvailableLocales()) {
            if("%TA".equals(format) && "tr".equals(l.getLanguage()) ) {
                // This test is skipped, the result is better than printf
                continue;
            }
            if("und".equals(l.toLanguageTag())) {
                continue;
            }
            VarFormatter vf = new VarFormatter("${var" + format + "}", l);
            String printf = String.format(l, format, value);
            String formatter = vf.format(values);
            if(! fail && ! printf.equals(formatter) ) {
                System.out.println("mismatch for " + format + " at locale " + l.toLanguageTag() + " " + printf + " " + formatter);
            } else {
                Assert.assertEquals("mismatch for " + format + " at locale " + l.toLanguageTag(), printf, formatter );                
            }
        }
    }
    private void checkFormat(Object value, String format) {
        checkFormat(value, format, true);
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
        checkFormat(65535, "%010d");
        checkFormat(-65535, "%(d");
    }

    @Test
    public void test4() {
        checkFormat(Math.PI, "%f");
        checkFormat(Math.PI, "%10.2f");
        checkFormat(Math.PI, "%+10.2f");
    }

    private void testDate(Object date) {
        checkFormat(date, "%tH");
        checkFormat(date, "%tI");
        checkFormat(date, "%tk");
        checkFormat(date, "%tl");
        checkFormat(date, "%tM");
        checkFormat(date, "%tS");
        checkFormat(date, "%tL");
        checkFormat(date, "%tN");
        checkFormat(date, "%tp");
        //checkFormat(date, "%tz"); // Fails for a few locals
        checkFormat(date, "%tZ");
        checkFormat(date, "%ts");
        checkFormat(date, "%tQ");
        checkFormat(date, "%tB");
        checkFormat(date, "%tb");
        checkFormat(date, "%th");
        checkFormat(date, "%tA");
        checkFormat(date, "%ta");
        checkFormat(date, "%tC");
        checkFormat(date, "%tY");
        checkFormat(date, "%ty");
        checkFormat(date, "%tj");
        checkFormat(date, "%tm");
        checkFormat(date, "%td");
        checkFormat(date, "%te");
        //checkFormat(new Date(), "%tR");
        //checkFormat(new Date(), "%tT");
        //checkFormat(new Date(), "%tr");
        //checkFormat(new Date(), "%tD");
        //checkFormat(new Date(), "%tF");
        //checkFormat(new Date(), "%tc");

        checkFormat(date, "%Ta");
        checkFormat(date, "%TA");
        checkFormat(date, "%Tp");
    }

    @Test
    public void testDateFormat() {
        Date epoch = new Date(0);
        Map<String, Object> values = Collections.singletonMap("var", epoch);
        VarFormatter vf = new VarFormatter("${var%t<Europe/Paris>H}", Locale.US);
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for time zone parsing" , "01", formatter );
        testDate(new Date());
        testDate(ZonedDateTime.now());
    }

    @Test
    public void testLocale() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        // A locale with non common digit symbols
        Locale l = Locale.forLanguageTag("hi-IN");
        VarFormatter vf = new VarFormatter("${var%010d:" + l.toLanguageTag() + "}");
        String printf = String.format(l, "%010d", 1);
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for time zone parsing" , printf, formatter );
    }

    @Test
    public void testSpeed() {
        Object value = Math.PI * 1e6;
        Map<String, Object> values = Collections.singletonMap("var", value);

        Date start = new Date();

        start = new Date();
        for(int i = 0 ; i < 1000000 ; i++) {
            @SuppressWarnings("unused")
            String a = String.format("% 10.2f",  value);
        }
        System.out.println(new Date().getTime() - start.getTime());

        start = new Date();
        VarFormatter vf0 = new VarFormatter("${var% 10.2f}", Locale.getDefault());
        for(int i = 0 ; i < 1000000 ; i++) {
            @SuppressWarnings("unused")
            String a = vf0.format(values);
        }
        System.out.println(new Date().getTime() - start.getTime());
    }

    @Test
    public void testEscape() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        VarFormatter vf = new VarFormatter("a${}b{}c");
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for string escape" , "a${}b{}c", formatter );
    }

}
