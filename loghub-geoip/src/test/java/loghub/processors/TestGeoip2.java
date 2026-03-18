package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

class TestGeoip2 {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.pipeline.geoip");
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCityAll(GeoipRunner runner) throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
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
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
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
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(new String[]{"name", "code", "country"});
        });
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(2, geoinfos.size());
        @SuppressWarnings("unchecked")
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        Assertions.assertEquals(2, country.size());
        Assertions.assertEquals("United States", country.get("name"));
        Assertions.assertEquals("US", country.get("code"));
        String postal = (String) geoinfos.get("postal");
        Assertions.assertEquals("94040", postal);
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testProcessCountry(GeoipRunner runner) throws ProcessorException {
        Geoip2 geoip = build(b -> b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-Country.mmdb").toString()));
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(Map.of("code", "NA", "name", "North America"), geoinfos.remove("continent"));
        Assertions.assertEquals(Map.of("code", "US", "name", "United States"), geoinfos.remove("country"));
        Assertions.assertEquals(0, geoinfos.size());
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    void testExtendedFormat(GeoipRunner runner) throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setExtendedFormat(true);
            b.setIsoCodeKey("iso");
            b.setTypes(new String[]{"all"});
        });
        Map<String, Object> geoinfos = runner.apply(geoip, factory);

        @SuppressWarnings("unchecked")
        Map<String, Object> country = (Map<String, Object>) geoinfos.remove("country");
        Assertions.assertEquals(Set.of("iso", "name", "geoname_id"), country.keySet());
        Map<String, Object> continent = (Map<String, Object>) geoinfos.remove("continent");
        Assertions.assertEquals(Set.of("code", "name", "geoname_id"), continent.keySet());
        Map<String, Object> city = (Map<String, Object>) geoinfos.remove("city");
        Assertions.assertEquals(Set.of("name", "geoname_id"), city.keySet());
    }

    private Geoip2 build(Consumer<Geoip2.Builder> builderTweaks) {
        Properties props = new Properties(Collections.emptyMap());
        Geoip2.Builder builder = Geoip2.getBuilder();
        builder.setField(VariablePath.parse("ip"));
        builder.setDestination(VariablePath.parse("geoip"));
        builder.setLocale("en");
        builder.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-Country.mmdb").toString());
        builder.setCacheManager(props.cacheManager);
        builderTweaks.accept(builder);
        Geoip2 geoip = builder.build();
        geoip.configure(props);
        return geoip;
    }

    @ParameterizedTest
    @EnumSource(GeoipRunner.class)
    @SuppressWarnings("unchecked")
    void parseConfig(GeoipRunner runner) throws IOException, ProcessorException {
        String geoipPath = TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString();
        String config = String.format("pipeline[geoip]{loghub.processors.Geoip2 {geoipdb: \"%s\", field: [ip], types: [\"all\"], destination: [geoip]}}", geoipPath);
        Properties conf = Configuration.parse(new StringReader(config));
        Geoip2 geoip = conf.namedPipeLine.get("geoip").processors.stream().findAny().map(Geoip2.class::cast).orElseThrow(() -> new IllegalStateException("No received defined"));
        geoip.configure(conf);
        Map<String, Object> geoinfos = runner.apply(geoip, factory);
        Assertions.assertEquals(7, geoinfos.size());
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        Assertions.assertEquals("US", country.get("code"));
    }

    @Test
    void test_loghub_processors_Geoip2() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Geoip2"
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
                , BeanChecks.BeanInfo.build("extendedFormat", Boolean.TYPE)
        );
    }

}
