package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;

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

public class TestUrlDecoders {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.DecodeUrl");
    }

    @Test
    public void testUrlDecoder() {
        DecodeUrl t = DecodeUrl.getBuilder().build();
        t.setFields(new String[]{"*"});
        Event e = factory.newEvent();
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("\"Paints\" Oudalan", e.get("q"));
        Assert.assertEquals("%20", e.get("userAgent"));
    }

    @Test
    public void testUrlDecoderLoop() {
        DecodeUrl.Builder builder = DecodeUrl.getBuilder();
        builder.setFields(new String[]{"userAgent"});
        builder.setLoop(true);
        DecodeUrl t = builder.build();
        Event e = factory.newEvent();
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("%22Paints%22+Oudalan", e.get("q"));
        Assert.assertEquals(" ", e.get("userAgent"));
    }

    @Test
    public void testFailedStrict() {
        DecodeUrl.Builder builder = DecodeUrl.getBuilder();
        builder.setField(VariablePath.parse("userAgent"));
        builder.setLoop(true);
        builder.setStrict(true);
        DecodeUrl t = builder.build();
        Event e = factory.newEvent();
        e.put("userAgent", "%__");
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("ERROR", status.status.get(status.status.size() -1));
        Assert.assertEquals("%__", e.get("userAgent"));
    }

    @Test
    public void testFailedNotStrict() {
        DecodeUrl.Builder builder = DecodeUrl.getBuilder();
        builder.setField(VariablePath.parse("userAgent"));
        builder.setLoop(true);
        builder.setStrict(false);
        DecodeUrl t = builder.build();
        Event e = factory.newEvent();
        e.put("userAgent", "%__");
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("CONTINUE", status.status.get(status.status.size() -1));
        Assert.assertEquals("%__", e.get("userAgent"));
    }

    @Test
    public void test_loghub_processors_DecodeUrl() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.DecodeUrl"
                , BeanChecks.BeanInfo.build("encoding", String.class)
                , BeanChecks.BeanInfo.build("loop", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("strict", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("depth", Integer.TYPE)
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
