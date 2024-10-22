package loghub.processors;

import java.beans.IntrospectionException;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestLog {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testLog() throws IOException {
        String confile = "pipeline[main] {log(\"fixed message\", FATAL) | log(\"${%s}\", ERROR)} timezone: \"UTC\"";
        Properties conf = Tools.loadConf(new StringReader(confile));
        Helpers.parallelStartProcessor(conf);
        List<LogEvent> events = new ArrayList<>();
        try (CharArrayWriter baos = new CharArrayWriter()) {
            addAppender(baos, events);
            Event event = factory.newTestEvent();
            event.setTimestamp(new Date(0));
            Tools.runProcessing(event, conf.namedPipeLine.get("main"), conf);
        }

        LogEvent fatal = events.get(0);
        Assert.assertEquals(Level.FATAL, fatal.getLevel());
        Assert.assertEquals("loghub.pipeline.main", fatal.getLoggerName());
        Assert.assertEquals("fixed message", fatal.getMessage().getFormattedMessage());

        LogEvent error = events.get(1);
        Assert.assertEquals(Level.ERROR, error.getLevel());
        Assert.assertEquals("loghub.pipeline.main", error.getLoggerName());
        Assert.assertEquals("[Thu Jan 01 00:00:00 UTC 1970]{}#{}", error.getMessage().getFormattedMessage());
    }

    @Test
    public void test_loghub_processors_Log() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Log"
                , BeanChecks.BeanInfo.build("message", Expression.class)
                , BeanChecks.BeanInfo.build("level", String.class)
        );
    }

    void addAppender(Writer writer, List<LogEvent> events) {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        StringLayout layout = new StringLayout() {
            @Override
            public void encode(LogEvent source, ByteBufferDestination destination) {

            }

            @Override
            public byte[] getFooter() {
                return new byte[0];
            }

            @Override
            public byte[] getHeader() {
                return new byte[0];
            }

            @Override
            public byte[] toByteArray(LogEvent event) {
                return new byte[0];
            }

            @Override
            public String toSerializable(LogEvent event) {
                events.add(event.toImmutable());
                return "";
            }

            @Override
            public String getContentType() {
                return null;
            }

            @Override
            public Map<String, String> getContentFormat() {
                return null;
            }

            @Override
            public Charset getCharset() {
                return StandardCharsets.UTF_8;
            }
        };
        Appender appender = WriterAppender.createAppender(layout, null, writer, "writerName", false, true);
        appender.start();
        config.addAppender(appender);
        config.getRootLogger().addAppender(appender, null, null);
        Configurator.setRootLevel(Level.ERROR);
    }

}
