package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestDissect {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void test() throws ProcessorException {
        dobuild("%{a} %{b} %{c}", "1 2 3", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));

        // Right padding modifier ->
        dobuild("%{a->} %{b} %{c}", "1   2 3", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{a->},%{b},%{c}", "1,,,,2,3", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{a->},:%{b},%{c}", "1,:,:,:,:2,3", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{->},%{b},%{c}", "1,,,,2,3", Map.ofEntries(Map.entry("b", "2"), Map.entry("c", "3")));

        // Append modifier +
        dobuild("%{a} %{+a} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", "123")));
        dobuild("%{a} %{+a} %{+a}", b -> b.setAppendSeparator(", "), "1 2 3", Map.ofEntries(Map.entry("a", "1, 2, 3")));

        // Append modifier with order + with /n
        dobuild("%{a} %{+a/2} %{+a/1}", "1 2 3", Map.ofEntries(Map.entry("a", "132")));

        // Named skip key ?
        dobuild("%{a} %{?skipme} %{c}", "1 2 3", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "3")));
        dobuild("%{a} %{?skipme} %{?skipme} %{c}", "1 2 3 4", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "4")));

        // Reference keys * and &
        dobuild("%{*a} %{b} %{&a}", "c 1 2", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2")));
        dobuild("%{&a} %{b} %{*a}", "2 1 c", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2")));

        // Remaining match
        dobuild("%{a} %{b},%{c}", "1 2,3  4", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3  4")));

        // Consecutive repeating delimiters
        dobuild("%{a},%{b},%{c},%{d}", "1,,,4", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", ""), Map.entry("d", "4")));
        dobuild("%{a},%{b},%{c},%{d},%{e},%{f},%{g}", "1,,2,,,,3", Map.ofEntries(
                Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", "2"),
                Map.entry("d", ""), Map.entry("e", ""), Map.entry("f", ""),
                Map.entry("g", "3")));

        // Postfix pattern
        dobuild("%{timestamp} %{+timestamp} %{+timestamp} %{logsource} %{program}[%{pid}]: %{message}",
                b -> b.setAppendSeparator(" "),
                "Mar 16 00:01:25 example postfix/smtpd[1713]: connect from example.com[192.100.1.3]",
                Map.ofEntries(Map.entry("timestamp", "Mar 16 00:01:25"), Map.entry("pid", "1713"), Map.entry("program", "postfix/smtpd"), Map.entry("logsource", "example"), Map.entry("message", "connect from example.com[192.100.1.3]")));

        // More tests
        dobuild("0%{a} %{b} %{c}4", "01 2 34", Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{+a/2} %{+a/1} %{+a/0}", "1 2 3", Map.ofEntries(Map.entry("a", "321")));
    }

    @Test
    public void fails() throws ProcessorException {
        checkException(IllegalArgumentException.class, "%{/2}", "Append order defined, without append modifier");
        checkException(IllegalArgumentException.class, "%{a:nothing}", "Unknown conversion type: nothing");
        checkException(ProcessorException.class, "%{a:int}", "Can not convert \"a\" to int");
        checkException(IllegalArgumentException.class, "%{a} %{+a/2}", "Appender modifier out of range for key \"a\"");
        checkException(IllegalArgumentException.class, "%{a} %{+a:int}", "Appender only works with Strings for key \"a\"");
        checkException(IllegalArgumentException.class, "%{a} %{*a}", "Reference key \"a\" must be specified as pair");
        checkException(IllegalArgumentException.class, "%{*a} %{*a}", "Needs a reference name and a reference value for key \"a\"");
        checkException(IllegalArgumentException.class, "%{&a} %{&a}", "Needs a reference name and a reference value for key \"a\"");
        checkException(IllegalArgumentException.class, "%{*a:int} %{&a}", "A type conversion can't be applied to a reference name");

        dobuild("prefix%{a} %{b}", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
        dobuild("%{a} %{b}suffix", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
        dobuild("%{a} %{b} %{c}", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
    }

    private <E extends Exception> void checkException(Class<E> eClass, String pattern, String expectedMessage) {
        checkException(eClass, pattern, "", expectedMessage);
    }

    private <E extends Exception> void checkException(Class<E> eClass, String pattern, String message, String expectedMessage) {
        E ex= Assert.assertThrows(eClass, () -> dobuild(pattern, message, Map.ofEntries()));
        Assert.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void convert() throws ProcessorException, UnknownHostException {
        dobuild("%{a:int} %{b:ip} %{c:boolean}", "1 8.8.8.8 true",
                Map.ofEntries(Map.entry("a", 1), Map.entry("b", InetAddress.getByName("8.8.8.8")), Map.entry("c", true)));
    }

    @Test
    public void inPlaceWithPath() throws ProcessorException {
        Dissect.Builder builder = Dissect.getBuilder();
        builder.setPattern("%{a.b} %{c.d} %{#meta}");
        builder.setInPlace(true);
        builder.setField(VariablePath.of("message"));
        Dissect dissector = builder.build();
        Event ev = factory.newEvent();
        ev.put("message", "1 2 3");
        ev.process(dissector);
        Assert.assertEquals("1", ev.getAtPath(VariablePath.parse("a.b")));
        Assert.assertEquals("2", ev.getAtPath(VariablePath.parse("c.d")));
        Assert.assertEquals("3", ev.getAtPath(VariablePath.parse("#meta")));
    }

    private void dobuild(String pattern, Consumer<Dissect.Builder> configure,  String value, Object expected) throws ProcessorException {
        Dissect.Builder builder = Dissect.getBuilder();
        builder.setPattern(pattern);
        builder.setInPlace(false);
        configure.accept(builder);
        Dissect dissector = builder.build();
        Event ev = factory.newEvent();
        Object values = dissector.fieldFunction(ev, value);
        Assert.assertEquals(expected, values);
    }

    private void dobuild(String pattern, String value, Object expected) throws ProcessorException {
        dobuild(pattern, b -> {}, value, expected);
    }

    @Test
    public void test_loghub_processors_Dissect() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Dissect"
                , BeanChecks.BeanInfo.build("pattern", String.class)
                , BeanChecks.BeanInfo.build("appendSeparator", String.class)
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
