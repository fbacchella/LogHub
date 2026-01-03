package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.CacheManager;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestGeoipNext {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.pipeline");
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCityAll(GeoipRunner runner) throws ProcessorException {
        GeoipNext geoip = build(b -> {
            b.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(List.of("all").toArray(new String[0]));
        } );
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(7, geoinfos.size(), "not enough elements");
        Assertions.assertTrue(geoinfos.containsKey("continent"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> subdivisions = (List<Map<String, Object>>) geoinfos.get("subdivisions");
        Assertions.assertEquals(1, subdivisions.size());
        Assertions.assertEquals("California", subdivisions.getFirst().get("name"));
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCityFiltered(GeoipRunner runner) throws ProcessorException {
        GeoipNext geoip = build(b -> {
            b.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(List.of("all", "-continent").toArray(new String[0]));
        } );
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(6, geoinfos.size(), "not enough elements");
        Assertions.assertFalse(geoinfos.containsKey("continent"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> subdivisions = (List<Map<String, Object>>) geoinfos.get("subdivisions");
        Assertions.assertEquals(1, subdivisions.size());
        Assertions.assertEquals("California", subdivisions.getFirst().get("name"));
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCityCountry(GeoipRunner runner) throws ProcessorException {
        GeoipNext geoip = build(b -> {
            b.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(new String[]{"name", "code", "country"});
            b.setKeepOldLayout(false);
        });
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(1, geoinfos.size());
        @SuppressWarnings("unchecked")
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        Assertions.assertEquals(2, country.size());
        Assertions.assertEquals("United States", country.get("name"));
        Assertions.assertEquals("US", country.get("code"));
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCountry(GeoipRunner runner) throws ProcessorException {
        GeoipNext geoip = build(b -> b.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-Country.mmdb").toString()));
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(2, geoinfos.size());
        Assertions.assertEquals("North America", geoinfos.get("continent"));
    }

    @Test
    void testEmpty() throws ProcessorException {
        GeoipNext geoip = build(b -> b.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-Country.mmdb").toString()));
        Event e = factory.newEvent();
        e.put("ip", "169.254.1.1");
        geoip.process(e);
        Assertions.assertFalse(e.containsKey("geoip"));
    }

    private GeoipNext build(Consumer<GeoipNext.Builder> builderTweaks) {
        Properties props = new Properties(Collections.emptyMap());
        GeoipNext.Builder builder = GeoipNext.getBuilder();
        builder.setField(VariablePath.parse("ip"));
        builder.setDestination(VariablePath.parse("geoip"));
        builder.setLocale("en");
        builder.setGeoipdb(TestGeoipNext.class.getResource("/GeoLite2-Country.mmdb").toString());
        builder.setCacheManager(props.cacheManager);
        builderTweaks.accept(builder);
        GeoipNext geoip = builder.build();
        geoip.configure(props);
        return geoip;
    }

    @Test
    void test_loghub_processors_GeoipNext() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.GeoipNext"
                , BeanChecks.BeanInfo.build("geoipdb", String.class)
                , BeanChecks.BeanInfo.build("types", String[].class)
                , BeanChecks.BeanInfo.build("locale", String.class)
                , BeanChecks.BeanInfo.build("cacheSize", Integer.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("cacheManager", CacheManager.class)
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
