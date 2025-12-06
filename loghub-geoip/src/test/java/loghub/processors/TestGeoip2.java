package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import loghub.configuration.CacheManager;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGeoip2 {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    private static final InetAddress google;

    static {
        try {
            google = Inet4Address.getByName("8.8.8.8");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testProcessCityAll() throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(List.of("all").toArray(new String[0]));
        } );
        Map<Object, Object> geoinfos = runString(geoip);
        assertEquals("not enough elements", 7, geoinfos.size());
        assertTrue(geoinfos.containsKey("continent"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> subdivisions = (List<Map<String, Object>>) geoinfos.get("subdivisions");
        assertEquals(1, subdivisions.size());
        assertEquals("California", subdivisions.getFirst().get("name"));
    }

    @Test
    public void testProcessCityFiltered() throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(List.of("all", "-continent").toArray(new String[0]));
        } );
        Map<Object, Object> geoinfos = runString(geoip);
        assertEquals("not enough elements", 6, geoinfos.size());
        assertFalse(geoinfos.containsKey("continent"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> subdivisions = (List<Map<String, Object>>) geoinfos.get("subdivisions");
        assertEquals(1, subdivisions.size());
        assertEquals("California", subdivisions.getFirst().get("name"));
    }

    @Test
    public void testProcessCityCountry() throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(new String[]{"name", "code", "country"});
            b.setKeepOld(false);
        });
        Map<Object, Object> geoinfos = runString(geoip);
        assertEquals(1, geoinfos.size());
        @SuppressWarnings("unchecked")
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        assertEquals(2, country.size());
        assertEquals("United States", country.get("name"));
        assertEquals("US", country.get("code"));
    }

    @Test
    public void testProcessCountry() throws ProcessorException {
        Geoip2 geoip = build(b -> b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-Country.mmdb").toString()));
        Map<Object, Object> geoinfos = runString(geoip);
        System.err.println(geoinfos);
        assertEquals(2, geoinfos.size());
        assertEquals("North America", geoinfos.get("continent"));
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

    private Map<Object, Object> runString(Geoip2 geoip) throws ProcessorException {
        Event e = factory.newEvent();
        e.put("ip", "8.8.8.8");

        geoip.process(e);
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        return geoinfos;
    }

    private Map<Object, Object> runIP(Geoip2 geoip) throws ProcessorException {
        Event e = factory.newEvent();
        e.put("ip", google);

        geoip.process(e);
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        return geoinfos;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void parseConfig() throws IOException, ProcessorException {
        String geoipPath = TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString();
        String config = String.format("pipeline[geoip]{loghub.processors.Geoip2 {geoipdb: \"%s\", field: [ip], types: [\"all\"], destination: [geoip]}}", geoipPath);
        Properties conf = Configuration.parse(new StringReader(config));
        Geoip2 geoip = conf.namedPipeLine.get("geoip").processors.stream().findAny().map(Geoip2.class::cast).orElseThrow(() -> new IllegalStateException("No received defined"));
        geoip.configure(conf);
        // Resolve a String
        Map<Object, Object> geoinfos = runString(geoip);
        System.err.println(geoinfos);
        assertEquals(7, geoinfos.size());
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        assertEquals("US", country.get("code"));

        // Resolve an InetAddress
        geoinfos = runIP(geoip);
        assertEquals(7, geoinfos.size());
        country = (Map<String, String>) geoinfos.get("country");
        assertEquals("US", country.get("code"));
    }

    @Test
    public void test_loghub_processors_Geoip2() throws IntrospectionException, ReflectiveOperationException {
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
        );
    }

}
