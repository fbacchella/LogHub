package loghub;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestVarFormatter {

    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
        Configurator.setLevel("loghub.VarFormatter", Level.DEBUG);
    }

    private void checkFormat(Object value, String format, boolean fail) {
        Map<String, Object> values = Collections.singletonMap("var", value);
        for(Locale l: Locale.getAvailableLocales()) {
            // String.format don't handle well the turkish i (not i), like Pazartesi -> PAZARTESİ
            // So skip it
            if ("tr".equals(l.getLanguage()) && "%TA".equals(format)) {
                continue;
            }
            // String.format don't handle well the Azerbaijan i (not i), like BAZAR ERTƏSI -> BAZAR ERTƏSİ
            // So skip it
            if ("az".equals(l.getLanguage()) && "%TA".equals(format)) {
                continue;
            }
            VarFormatter vf = new VarFormatter("${var" + format + "}", l);
            String printf = String.format(l, format, value);
            String formatter = vf.format(values);
            if(! fail && ! printf.equals(formatter) ) {
                System.out.println("mismatch for " + format + " at locale " + l.toLanguageTag() + " " + printf + " " + formatter);
            } else {
                Assert.assertEquals("mismatch for " + format + " at locale " + l.toLanguageTag() + " with " + value.getClass().getSimpleName(), printf, formatter );
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
        checkFormat(date, "%tz");
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
        long printf = new Date().getTime() - start.getTime();

        start = new Date();
        VarFormatter vf0 = new VarFormatter("${var% 10.2f}", Locale.getDefault());
        for(int i = 0 ; i < 1000000 ; i++) {
            @SuppressWarnings("unused")
            String a = vf0.format(values);
        }
        long varformatter = new Date().getTime() - start.getTime();
        Assert.assertTrue(varformatter < printf);
    }

    @Test
    public void testEscape() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        VarFormatter vf = new VarFormatter("a${}b{}c{.}d");
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for string escape", "a${}b{}c{.}d", formatter );

        vf = new VarFormatter("a'c");
        formatter = vf.format(values);
        Assert.assertEquals("mismatch for string escape", "a'c", formatter );
    }

    @Test
    public void testMany() {
        Map<String, Object> values = new HashMap<String, Object>(){{
            put("a", 2);
            put("b", 1);
        }};
        VarFormatter vf = new VarFormatter("'${a}${}${b}'");
        Assert.assertEquals("mismatch for complex pattern" , "'2${}1'", vf.format(values) );
    }

    @Test
    public void formatSimple() {
        VarFormatter vf = new VarFormatter("${%d} ${%04d}");
        Assert.assertEquals("mismatch for complex pattern", "123 0123", vf.format(123) );
    }

    @Test
    public void formatPath() {
        VarFormatter vf = new VarFormatter("${a.b}", Locale.ENGLISH);
        Map<String,Map<String, Object>> obj = Collections.singletonMap("a", Collections.singletonMap("b", "c"));
        String formatted = vf.format(obj);
        Assert.assertEquals("c", formatted);
    }

    @Test
    public void formatArray() {
        VarFormatter vf = new VarFormatter("${#1%s} ${#1%s} ${#3%s}", Locale.ENGLISH);
        List<String> obj = Arrays.asList(new String[] {"1", "2", "3"});
        String formatted = vf.format(obj);
        Assert.assertEquals("1 1 3", formatted);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError1() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b}");
        vf.format(values);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError2() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b.c}");
        vf.format(values);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError3() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b.c}");
        vf.format(values);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError4() {
        VarFormatter vf = new VarFormatter("${#2%s}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        String formatted = vf.format(obj);
        Assert.assertEquals("1", formatted);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError5() {
        VarFormatter vf = new VarFormatter("${a} ${#2}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        String formatted = vf.format(obj);
        Assert.assertEquals("1", formatted);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError6() {
        VarFormatter vf = new VarFormatter("${a}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        vf.format(obj);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testError7() {
        VarFormatter vf = new VarFormatter("${#1}", Locale.ENGLISH);
        Map<String, Object> obj = Collections.singletonMap("a", 1);
        vf.format(obj);
    }

}
