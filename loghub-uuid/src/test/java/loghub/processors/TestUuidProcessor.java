package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.github.f4b6a3.uuid.UuidCreator;
import com.github.f4b6a3.uuid.enums.UuidLocalDomain;
import com.github.f4b6a3.uuid.enums.UuidNamespace;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.UuidProcessor.Version;

class TestUuidProcessor {

    private static final Logger logger = LogManager.getLogger();
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
    }

    @ParameterizedTest
    @EnumSource(value = Version.class)
    void testVersions(Version version) throws ProcessorException {
        UuidProcessor.Builder builder = UuidProcessor.getBuilder();
        builder.setField(VariablePath.of("uuid"));
        builder.setVersion(version);

        switch (version) {
            case V3, V5 -> {
                builder.setName(new Expression("github.fr"));
                builder.setNamespace(UuidNamespace.NAMESPACE_DNS);
            }
            case V3_CUSTOM, V5_CUSTOM -> {
                builder.setName(new Expression("github.fr"));
                builder.setNamespaceUuid(UuidNamespace.NAMESPACE_DNS.getValue().toString());
            }
            case V1_PROVIDED -> builder.setNodeIdentifier(new Expression(1));
            case V2 -> {
                builder.setLocalDomain(UuidLocalDomain.LOCAL_DOMAIN_PERSON);
                builder.setLocalNumber(new Expression(1));
            }
            case V4_SECURE, V7_SECURE -> builder.setRandomSource("SHA1PRNG");
            default -> { }
        }

        UuidProcessor uuid = builder.build();
        Assertions.assertTrue(uuid.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("f1", "test value");
        Assertions.assertTrue(uuid.process(event));
        UUID result = (UUID) event.get("uuid");
        Assertions.assertNotNull(result);

        int expectedVersion = switch (version) {
            case V1, V1_MAC, V1_HASH, V1_RANDOM, V1_PROVIDED -> 1;
            case V2 -> 2;
            case V3, V3_CUSTOM -> 3;
            case V4, V4_FAST, V4_SECURE -> 4;
            case V5, V5_CUSTOM -> 5;
            case V6 -> 6;
            case V7, V7_FAST, V7_SECURE -> 7;
        };
        Assertions.assertEquals(expectedVersion, result.version());

        if (version == Version.V3) {
            UUID refuuid = UuidCreator.getNameBasedMd5(UuidNamespace.NAMESPACE_DNS, "github.fr");
            Assertions.assertEquals(refuuid, result);
        } else if (version == Version.V5) {
            UUID refuuid = UuidCreator.getNameBasedSha1(UuidNamespace.NAMESPACE_DNS, "github.fr");
            Assertions.assertEquals(refuuid, result);
        }
    }

    @Test
    void testInvalidSecureRandom() {
        UuidProcessor.Builder builder = UuidProcessor.getBuilder();
        builder.setVersion(Version.V4_SECURE);
        builder.setRandomSource("InvalidSecureRandom");
        Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest
    @EnumSource(value = Version.class)
    void testParsing(Version version) throws IOException {
        String confile = """
            pipeline[defaultmessage] {
                loghub.processors.UuidProcessor {
                    version: "%s",
                    v7type: 2,
                    namespace: "NAMESPACE_DNS",
                    namespaceUuid: "4e2b4d4c-92e8-11ed-86a8-3fdb0085247e",
                    localDomain: "LOCAL_DOMAIN_PERSON",
                    randomSource: "NativePRNG",
                    field: [#uuid],
                    nodeIdentifier: 1,
                    name: "loghub.com",
                    localNumber: 1,
                }
            }""".formatted(version);
        Properties p =  Configuration.parse(new StringReader(confile));
        Event ev = factory.newEvent();
        Tools.runProcessing(ev, p.namedPipeLine.get("defaultmessage"), p);
        Assertions.assertInstanceOf(UUID.class, ev.getAtPath(VariablePath.ofMeta("uuid")));
    }

    @Test
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.UuidProcessor"
                , BeanChecks.BeanInfo.build("version", Version.class)
                , BeanChecks.BeanInfo.build("v7type", Integer.TYPE)
                , BeanChecks.BeanInfo.build("name", Expression.class)
                , BeanChecks.BeanInfo.build("nodeIdentifier", Expression.class)
                , BeanChecks.BeanInfo.build("namespace", UuidNamespace.class)
                , BeanChecks.BeanInfo.build("namespaceUuid", String.class)
                , BeanChecks.BeanInfo.build("localDomain", UuidLocalDomain.class)
                , BeanChecks.BeanInfo.build("randomSource", String.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }
}
