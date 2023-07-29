package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
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
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static org.junit.Assert.assertEquals;

public class TestGeoip2 {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testProcessCityAll() throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(new String[0]);
        } );
        Map<Object, Object> geoinfos = run(geoip);
        assertEquals("not enough elements", 7, geoinfos.size());
    }

    @Test
    public void testProcessCityCountry() throws ProcessorException {
        Geoip2 geoip = build(b -> {
            b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-City.mmdb").toString());
            b.setTypes(new String[]{Geoip2.LocationType.COUNTRY.name()});
        });
        Map<Object, Object> geoinfos = run(geoip);
        assertEquals(1, geoinfos.size());
        @SuppressWarnings("unchecked")
        Map<String, String> country = (Map<String, String>) geoinfos.get("country");
        assertEquals("United States", country.get("name"));
        assertEquals("US", country.get("code"));
    }

    @Test
    public void testProcessCountry() throws ProcessorException {
        Geoip2 geoip = build(b -> b.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-Country.mmdb").toString()));
        Map<Object, Object> geoinfos = run(geoip);
        assertEquals(3, geoinfos.size());
        assertEquals("North America", geoinfos.get("continent"));
    }

    private Geoip2 build(Consumer<Geoip2.Builder> builderTweaks) {
        Properties props = new Properties(Collections.emptyMap());
        Geoip2.Builder builder = Geoip2.getBuilder();
        builder.setField(VariablePath.parse("ip"));
        builder.setDestination(VariablePath.parse("geoip"));
        Geoip2.LocationType[] types = Geoip2.LocationType.values();
        String[] typesNames = new String[types.length];
        for(int i = 0 ; i < types.length ; i++) {
            typesNames[i] = types[i].name().toLowerCase();
        }
        builder.setTypes(typesNames);
        builder.setLocale("en");
        builder.setGeoipdb(TestGeoip2.class.getResource("/GeoLite2-Country.mmdb").toString());
        builderTweaks.accept(builder);
        Geoip2 geoip = builder.build();
        geoip.configure(props);
        return geoip;
    }

    private Map<Object, Object> run(Geoip2 geoip) throws ProcessorException {
        Event e = factory.newEvent();
        e.put("ip", "8.8.8.8");

        geoip.process(e);
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        return geoinfos;
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
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

/*    @Test
    public void testProcessGeneric() throws ProcessorException {
        Properties props = new Properties(Collections.emptyMap());
        Geoip2.Builder builder = Geoip2.getBuilder();
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.parse("geoip"));
        builder.setGeoipdb("/tmp/3ds-internal.mmdb");
        Geoip2.LocationType[] types = Geoip2.LocationType.values();
        String[] typesNames = new String[types.length];
        for(int i = 0 ; i < types.length ; i++) {
            typesNames[i] = types[i].name().toLowerCase();
        }
        builder.setTypes(typesNames);
        builder.setLocale("en");
        Geoip2 geoip = builder.build();
        Assert.assertTrue(geoip.configure(props));

        Event e = factory.newEvent();
        e.put("ip", "192.168.205.12");

        System.out.println(geoip.process(e));
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        System.out.println(geoinfos);
        assertEquals("not enough elements", 13, geoinfos.size());
    }
    
    @Test
    public void testProcessISP() throws ProcessorException {
        Properties props = new Properties(Collections.emptyMap());
        Geoip2.Builder builder = Geoip2.getBuilder();
        builder.setField(VariablePath.of(new String[] {"ip"}));
        builder.setDestination(VariablePath.of("geoip"));
        builder.setGeoipdb("/tmp/GeoIP2-ISP.mmdb");
        Geoip2.LocationType[] types = Geoip2.LocationType.values();
        String[] typesNames = new String[types.length];
        for(int i = 0 ; i < types.length ; i++) {
            typesNames[i] = types[i].name().toLowerCase();
        }
        builder.setTypes(typesNames);
        builder.setLocale("en");
        Geoip2 geoip = builder.build();
        geoip.configure(props);

        Event e = factory.newEvent();
        e.put("ip", "51.159.88.83");

        geoip.process(e);
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        System.out.println(geoinfos);
        assertEquals("not enough elements", 13, geoinfos.size());
    }
    
    @Test
    public void test3DS() throws IOException {
        Reader reader = new Reader(new File("/tmp/3ds-internal.mmdb"));
        System.out.println(reader.getMetadata());
        DatabaseRecord<Map> o = reader.getRecord(InetAddress.getByName("10.204.12.31"), Map.class);
        System.out.println(o.getNetwork());
        System.out.println(o.getData());
    }

    @Test
    public void testMaxMind() throws IOException {
        Reader reader = new Reader(new File("src/test/resources/GeoLite2-City.mmdb"));
        System.out.println(reader.getMetadata().getDatabaseType());
        DatabaseRecord<CityResponse> o = reader.getRecord(InetAddress.getByName("8.8.8.8"), CityResponse.class);
        System.out.println(o.getNetwork());
        System.out.println(o.getData());
    }
    
    @Test
    public void testISP() throws IOException {
        Reader reader = new Reader(new File("/tmp/GeoIP2-City.mmdb"));
        System.out.println(reader.getMetadata().getDatabaseType());
        DatabaseRecord<Map> o = reader.getRecord(InetAddress.getByName("51.159.88.83"), Map.class);
        System.out.println(o.getNetwork());
        System.out.println(o.getData());
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testCache() throws IOException, GeoIp2Exception {
        CachingProvider provider = Caching.getCachingProvider();
        javax.cache.CacheManager cacheManager = provider.getCacheManager();
        Cache2kBuilder<CacheKey, DecodedValue> builder2k = Cache2kBuilder.of(CacheKey.class, DecodedValue.class)
                .permitNullValues(false)
                .storeByReference(true)
                .keepDataAfterExpired(false)
                ;
        JmxSupport.enable(builder2k);
        ExtendedMutableConfiguration<CacheKey, DecodedValue> config = ExtendedMutableConfiguration.of(builder2k);
        Cache<CacheKey, DecodedValue> ehCache = cacheManager.createCache("somename", config);
        EntryProcessor<CacheKey, DecodedValue, DecodedValue> ep = this::extractValue;
        NodeCache nc = (k, l) -> ehCache.invoke(k, ep, l);
        DatabaseReader reader = new DatabaseReader.Builder(new File("test/resources/GeoLite2-City.mmdb")).withCache(nc).fileMode(FileMode.MEMORY_MAPPED).build();
        reader.city(InetAddress.getByName("8.8.8.8"));
    }

    @SuppressWarnings("rawtypes")
    private DecodedValue extractValue(MutableEntry<CacheKey, DecodedValue> i, Object[] j) {
        try {
            DecodedValue node;
            if (! i.exists()) {
                Loader loader = (Loader)j[0];
                node = loader.load(i.getKey());
                i.setValue(node);
            } else {
                node = i.getValue();
            }
            return node;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/

}
