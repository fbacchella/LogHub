package loghub.encoders;

import java.io.IOException;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;

public class TestMsgpack {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testParsing() {
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { forwardEvent: true}}}", i -> i.output());
    }

    @Test
    public void testParsingFailedBadBean() {
        thrown.expect(RecognitionException.class);
        thrown.expectMessage("Unknown bean 'notbean' for loghub.encoders.Msgpack");
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { notbean: 1}}}", i -> i.output());
    }

    @Test
    public void testParsingFailedbadType() {
        thrown.expect(RecognitionException.class);
        thrown.expectMessage("Invalid bean 'forwardEvent' for loghub.encoders.Msgpack: argument type mismatch");
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { forwardEvent: 1}}}", i -> i.output());
    }

}
