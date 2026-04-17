package loghub.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.TlsCiphers.CipherId;
import loghub.processors.TlsCiphers.Context;

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
        builder.setDestinationContext(TlsCiphers.Context.OPENSSL);
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        Assertions.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // TLS_AES_256_GCM_SHA384 is 0x13, 0x02
        event.put("cipher", "TLS_AES_256_GCM_SHA384");
        processor.process(event);
        // OpenSSL name is the same for this one in 02_openssl_ciphers.yaml
        Assertions.assertEquals("TLS_AES_256_GCM_SHA384", event.get("cipher"));

        // Test a cipher ONLY present in the CSV: TLS_ASCONAEAD128_ASCONHASH256 (0x00, 0x6E)
        event = factory.newEvent();
        event.put("cipher", "TLS_ASCONAEAD128_ASCONHASH256");
        processor.process(event);
        // Should return the name itself if no OpenSSL translation is found
        Assertions.assertEquals("TLS_ASCONAEAD128_ASCONHASH256", event.get("cipher"));

        // Test the first one in the CSV: TLS_NULL_WITH_NULL_NULL (0x00, 0x00)
        event = factory.newEvent();
        event.put("cipher", "TLS_NULL_WITH_NULL_NULL");
        processor.process(event);
        Assertions.assertEquals("TLS_NULL_WITH_NULL_NULL", event.get("cipher"));
    }

    @Test
    void testTranslationJava() throws ProcessorException {
        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setDestinationContext(TlsCiphers.Context.JAVA);
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

    @Test
    void testAutoDetection() throws ProcessorException {
        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setDestinationContext(TlsCiphers.Context.IANA);
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        Assertions.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // RSA_WITH_AES_128_CBC_SHA is OpenSSL name for TLS_RSA_WITH_AES_128_CBC_SHA
        event.put("cipher", "AES128-SHA"); // OpenSSL name
        processor.process(event);
        Assertions.assertEquals("TLS_RSA_WITH_AES_128_CBC_SHA", event.get("cipher"));
    }

    @Test
    void testOpensslCiphers() throws Exception {
        ProcessBuilder pb = new ProcessBuilder("openssl", "ciphers", "-V");
        Process p = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            Pattern linePattern = Pattern.compile("\\s+0x([0-9A-F]{2}),0x([0-9A-F]{2}) - (\\S+).*");
            TlsCiphers.Builder builder = TlsCiphers.getBuilder();
            builder.setDestinationContext(TlsCiphers.Context.IANA);
            TlsCiphers processor = builder.build();
            processor.setField(VariablePath.parse("cipher"));
            processor.configure(new Properties(Collections.emptyMap()));

            int count = 0;
            while ((line = reader.readLine()) != null) {
                Matcher m = linePattern.matcher(line);
                if (m.matches()) {
                    count++;
                    String opensslName = m.group(3);
                    Event event = factory.newEvent();
                    event.put("cipher", opensslName);
                    processor.process(event);
                    Assertions.assertNotEquals(FieldsProcessor.RUNSTATUS.FAILED, event.get("cipher"), "Failed to translate OpenSSL cipher: " + opensslName);
                }
            }
            logger.info("Validated {} ciphers from openssl ciphers -V", count);
        }
    }

    @Test
    void testJavaCiphers() throws Exception {
        SSLContext context = SSLContext.getDefault();
        String[] cipherSuites = context.getSupportedSSLParameters().getCipherSuites();
        Arrays.toString(cipherSuites);

        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setDestinationContext(TlsCiphers.Context.IANA);
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        processor.configure(new Properties(Collections.emptyMap()));

        int count = 0;
        for (String cipherSuite : cipherSuites) {
            // Skip SCSV ciphers as they are not real ciphers
            if (cipherSuite.contains("_SCSV")) {
                continue;
            }
            count++;
            Event event = factory.newEvent();
            event.put("cipher", cipherSuite);
            processor.process(event);
            Assertions.assertNotEquals(FieldsProcessor.RUNSTATUS.FAILED, event.get("cipher"), "Failed to translate Java cipher: " + cipherSuite);
        }
        logger.info("Validated {} ciphers from Java JSSE", count);
    }

    @Test
    void testOpensslDump() throws ProcessorException {
        TlsCiphers.Builder builder = TlsCiphers.getBuilder();
        builder.setDestinationContext(TlsCiphers.Context.IANA);
        TlsCiphers processor = builder.build();
        processor.setField(VariablePath.parse("cipher"));
        Assertions.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // TLS_CHACHA20_POLY1305_SHA256 is in openssldump.txt as 0x13,0x03
        // Its IANA name is also TLS_CHACHA20_POLY1305_SHA256
        event.put("cipher", "TLS_CHACHA20_POLY1305_SHA256");
        processor.process(event);
        Assertions.assertEquals("TLS_CHACHA20_POLY1305_SHA256", event.get("cipher"));

        // ECDHE-RSA-AES256-GCM-SHA384 (0xC0,0x30)
        event = factory.newEvent();
        event.put("cipher", "ECDHE-RSA-AES256-GCM-SHA384");
        processor.process(event);
        Assertions.assertEquals("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", event.get("cipher"));
    }

    @Test
    void testYamlConsistency() {
        Map<CipherId, Map<Context, String>> contextToIdToName = TlsCiphers.resolveId();

        for (Map.Entry<CipherId, Map<Context, String>> entry : contextToIdToName.entrySet()) {
            if (entry.getValue().size() == 4) {
                continue;
            }
            CipherId id = entry.getKey();
            Map<Context, String> currentIdToName = entry.getValue();
            if (!currentIdToName.containsKey(Context.IANA)) {
                logger.warn("Missing IANA key {} with names {}", id, currentIdToName);
            } else {
                Set<Context> missing = HashSet.newHashSet(4);
                for (Context c: Context.values()) {
                    if (! entry.getValue().containsKey(c)) {
                        missing.add(c);
                    }
                }
                logger.warn("Missing {} for {}/{}", missing, currentIdToName.get(Context.IANA), id);
            }
        }
    }

    @Test
    void parsingExtention(@TempDir Path dir) throws IOException {
        Path destination = dir.resolve("custom.yaml");
        Files.writeString(destination, """
- pk: 'BROKEN_TLS_RSA_AES_256_CCM'
  model: 'directory.Custom'
  fields:
    hex_byte_1: '0xc0'
    hex_byte_2: '0x9d'
        """);
        String conf = """
            pipeline[main] {
                loghub.processors.TlsCiphers {
                    destinationContext: "IANA",
                    extensions: ["%s"],
                }
            }
        """.formatted(destination);
        Properties p = Configuration.parse(new StringReader(conf));
        TlsCiphers m = (TlsCiphers) p.namedPipeLine.get("main").processors.stream().findFirst().get();
        Event event = factory.newEvent();
        Object realName = m.fieldFunction(event, "BROKEN_TLS_RSA_AES_256_CCM");
        Assertions.assertEquals("TLS_RSA_WITH_AES_256_CCM", realName);
    }

}
