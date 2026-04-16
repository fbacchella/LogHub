package loghub.processors;

import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import loghub.ProcessorException;

class TestTlsCiphers {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    void testTranslation() throws ProcessorException {
        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setSourceContext("iana");
        builder.setDestinationContext("openssl");
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        Assertions.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // TLS_AES_256_GCM_SHA384 is 0x13, 0x02
        event.put("cipher", "TLS_AES_256_GCM_SHA384");
        processor.process(event);
        // OpenSSL name is the same for this one in 02_openssl_ciphers.yaml
        Assertions.assertEquals("TLS_AES_256_GCM_SHA384", event.get("cipher"));
    }

    @Test
    void testTranslationJava() throws ProcessorException {
        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setSourceContext("iana");
        builder.setDestinationContext("java");
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        Assertions.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // TLS_RSA_WITH_AES_128_CBC_SHA is 0x00, 0x2f
        // In 01_iana: TLS_RSA_WITH_AES_128_CBC_SHA
        event.put("cipher", "TLS_RSA_WITH_AES_128_CBC_SHA"); 
        processor.process(event);
        // In 04_java: TLS_RSA_WITH_AES_128_CBC_SHA
        Assertions.assertEquals("TLS_RSA_WITH_AES_128_CBC_SHA", event.get("cipher"));
    }
}
