package loghub.processors;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.logging.log4j.Level;

import com.maxmind.db.CacheKey;
import com.maxmind.db.DecodedValue;
import com.maxmind.db.NodeCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.CacheManager;
import loghub.configuration.CacheManager.Policy;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Geoip2.Builder.class)
public class Geoip2 extends FieldsProcessor {

    enum LocationType {
        COUNTRY,
        REGISTREDCOUNTRY,
        REPRESENTEDCOUNTRY,
        CITY,
        LOCATION,
        CONTINENT,
        POSTAL,
        SUBDIVISION,
    }

    private static class LogHubNodeCache implements NodeCache {
        private final Cache<CacheKey, DecodedValue> cache;
        LogHubNodeCache(Cache<CacheKey, DecodedValue> cache) {
            this.cache = cache;
        }
        @Override
        public DecodedValue get(CacheKey key, Loader loader) {
            return cache.invoke(key, this::extractValue, loader);
        }
        public void reset() {
            cache.removeAll();
        }
        private DecodedValue extractValue(MutableEntry<CacheKey, DecodedValue> i, Object... j) {
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
                throw new EntryProcessorException(e);
            }
        }
    }

    public static class Builder extends FieldsProcessor.Builder<Geoip2> {
        @Setter
        private String geoipdb = null;
        @Setter
        private String[] types = new String[] {};
        @Setter
        private String locale = Locale.getDefault().getCountry();
        @Setter
        private int cacheSize = 100;
        @Setter
        private String refresh = "";
        @Setter
        private CacheManager cacheManager;
        public Geoip2 build() {
            return new Geoip2(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final URI geoipdb;
    private final LocationType[] types;
    @Getter
    private final String locale;
    @Getter
    private volatile Reader reader;
    private final LogHubNodeCache geoipCache;
    @Getter
    private final Duration delay;
    private final ReadWriteLock dbProtectionLock = new ReentrantReadWriteLock();
    private volatile long lastBuildDate = 0;

    public Geoip2(Builder builder) {
        super(builder);
        if (builder.types.length == 0) {
            this.types = Geoip2.LocationType.values();
        } else {
            this.types = Arrays.stream(builder.types)
                               .map(s -> s.toUpperCase(Locale.ENGLISH))
                               .map(LocationType::valueOf)
                               .toArray(LocationType[]::new);
        }
        this.geoipdb = Optional.ofNullable(builder.geoipdb)
                               .map(Helpers::fileUri)
                               .orElse(null);
        this.locale = Locale.forLanguageTag(builder.locale).getLanguage();
        if (!builder.refresh.trim().isEmpty()) {
            this.delay = Duration.parse(builder.refresh);
        } else {
            this.delay = null;
        }
        @SuppressWarnings("rawtypes")
        Cache<CacheKey, DecodedValue> cache = builder.cacheManager.getBuilder(CacheKey.class, DecodedValue.class)
                                                      .setCacheSize(builder.cacheSize)
                                                      .setName("Geoip2", this)
                                                      .setExpiry(Policy.ETERNAL)
                                                      .build();
        this.geoipCache = new LogHubNodeCache(cache);
    }

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        try {
            dbProtectionLock.readLock().lockInterruptibly();
        } catch (InterruptedException e) {
            logger.error("Interrupted");
            Thread.currentThread().interrupt();
            return RUNSTATUS.FAILED;
        }
        try {
            if (reader == null) {
                return RUNSTATUS.FAILED;
            }
            return resolution(event, addr);
        } finally {
            dbProtectionLock.readLock().unlock();
        }
    }

    private Object resolution(Event event, Object addr) throws ProcessorException {
        InetAddress ipInfo;
        if (addr instanceof InetAddress) {
            ipInfo = (InetAddress) addr;
        } else if (addr instanceof String) {
            try {
                ipInfo = Helpers.parseIpAddress((String) addr);
                if (ipInfo == null) {
                    throw event.buildException("can't read IP address " + addr);
                }
            } catch (UnknownHostException e) {
                throw event.buildException("can't read IP address " + addr, e);
            }
        } else {
            throw event.buildException("It's not an IP address: " + addr);
        }

        Country country;
        Country registredCountry;
        Country representedCountry;
        City city = null;
        Continent continent;
        Location location = null;
        Postal postal = null;
        List<Subdivision> subdivision = null;

        Map<String, Object> informations = new HashMap<>();
        try {
            switch (reader.getMetadata().getDatabaseType()) {
            case "GeoIP2-City":
            case "GeoLite2-City": {
                CityResponse response = reader.getRecord(ipInfo, CityResponse.class).getData();
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.getCountry();
                city = response.getCity();
                continent = response.getContinent();
                location = response.getLocation();
                postal = response.getPostal();
                registredCountry = response.getRegisteredCountry();
                representedCountry = response.getRepresentedCountry();
                subdivision = response.getSubdivisions();
                break;
            }
            case "GeoIP2-Country":
            case "GeoLite2-Country": {
                CountryResponse response = reader.getRecord(ipInfo, CountryResponse.class).getData();
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.getCountry();
                continent = response.getContinent();
                registredCountry = response.getRegisteredCountry();
                representedCountry = response.getRepresentedCountry();
                break;
            }
            default:
                Map<?, ?> response = reader.getRecord(ipInfo, Map.class).getData();
                return response == null ? RUNSTATUS.FAILED : response;
            }
        } catch (IOException e) {
            throw event.buildException("Can't read GeoIP database", e);
        }

        for (LocationType type: types) {
            switch(type) {
            case COUNTRY:
                if (country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(country.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(country.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if (!infos.isEmpty()) {
                        informations.put("country", infos);
                    }
                }
                break;
            case REPRESENTEDCOUNTRY:
                if (representedCountry != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(representedCountry.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(representedCountry.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if (!infos.isEmpty()) {
                        informations.put("represented_country", infos);
                    }
                }
                break;
            case REGISTREDCOUNTRY:
                if (registredCountry != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(registredCountry.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(registredCountry.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if (!infos.isEmpty()) {
                        informations.put("registred_country", infos);
                    }
                }
                break;
            case CITY: {
                if (city != null) {
                    Optional.ofNullable(city.getNames().get(locale)).ifPresent(i -> informations.put("city", i));
                }
                break;
            }
            case LOCATION:
                Map<String, Object> infos = new HashMap<>(7);
                if (location != null) {
                    Optional.ofNullable(location.getLatitude()).ifPresent(i -> infos.put("latitude", i));
                    Optional.ofNullable(location.getLongitude()).ifPresent(i -> infos.put("longitude", i));
                    Optional.ofNullable(location.getTimeZone()).ifPresent(i -> infos.put("timezone", i));
                    Optional.ofNullable(location.getAccuracyRadius()).ifPresent(i -> infos.put("accuray_radius", i));
                    Optional.ofNullable(location.getMetroCode()).ifPresent(i -> infos.put("metro_code", i));
                    Optional.ofNullable(location.getAverageIncome()).ifPresent(i -> infos.put("average_income", i));
                    Optional.ofNullable(location.getPopulationDensity()).ifPresent(i -> infos.put("population_density", i));
                    if (!infos.isEmpty()) {
                        informations.put("location", infos);
                    }
                }
                break;
            case CONTINENT:
                if (continent != null) {
                    Optional.ofNullable(continent.getNames().get(locale)).ifPresent(i -> informations.put("continent", i));
                }
                break;
            case POSTAL:
                if (postal != null) {
                    Optional.ofNullable(postal.getCode()).ifPresent(i -> informations.put("postal", i));
                }
                break;
            case SUBDIVISION:
                if (subdivision != null) {
                    List<Map<String, Object>> all = new ArrayList<>(subdivision.size());
                    for (Subdivision sub: subdivision) {
                        Map<String, Object> subdivisioninfo = new HashMap<>(2);
                        Optional.ofNullable(sub.getIsoCode()).ifPresent(i -> subdivisioninfo.put("code", i));
                        Optional.ofNullable(sub.getNames().get(locale)).ifPresent(i -> subdivisioninfo.put("name", i));
                        if (!subdivisioninfo.isEmpty()) {
                            all.add(subdivisioninfo);
                        }
                    }
                    if (!all.isEmpty()) {
                        informations.put("subdivisions", all);
                    }
                }
                break;
            }
        }
        if (! informations.isEmpty()) {
            return informations;
        } else {
            return RUNSTATUS.FAILED;
        }
    }

    @Override
    public boolean configure(Properties properties) {
        refresh();
        if (delay != null) {
            properties.registerScheduledTask("refreshgeoip", this::refresh, delay.toMillis());
        }
        return reader != null && super.configure(properties);
    }

    private void refresh() {
        Object newContent;
        try {
            newContent = readNewDb();
        } catch (IOException e) {
            logger.error("Unable to read the GeoIP database content: {}", () -> Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return;
        }
        if (newContent != null) {
            try {
                dbProtectionLock.writeLock().lockInterruptibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Give up the lock tentative in case of interruption
                return;
            }
            try {
                geoipCache.reset();
                if (reader != null) {
                    reader.close();
                }
                if (newContent instanceof File) {
                    reader = new Reader((File) newContent, geoipCache);
                } else {
                    try (InputStream is = new ByteArrayInputStream((byte[])newContent)) {
                        reader = new Reader(is, geoipCache);
                    }
                }
                lastBuildDate = reader.getMetadata().getBuildDate().getTime();
            } catch (IOException e) {
                logger.error("Unable to read the GeoIP database content: {}", () -> Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } finally {
                dbProtectionLock.writeLock().unlock();
            }
        }
    }

    private Object readNewDb() throws IOException {
        byte[] content = null;
        Reader temproraryReader = null;
        if (geoipdb != null && "file".equals(geoipdb.getScheme())) {
            temproraryReader = new Reader(new File(geoipdb.getPath()));
        } else if (geoipdb != null) {
            try (InputStream is = geoipdb.toURL().openStream()) {
                content = is.readAllBytes();
                temproraryReader = new Reader(new ByteArrayInputStream(content));
            }
        } else {
            try (InputStream is = Geoip2.class.getResourceAsStream("GeoLite2-City.mmdb")) {
                if (is == null) {
                    logger.error("Didn't find a default database");
                } else {
                    content = is.readAllBytes();
                    temproraryReader = new Reader(new ByteArrayInputStream(content));
                }
            }
        }
        if (temproraryReader != null) {
            try {
                if (temproraryReader.getMetadata().getBuildDate().getTime() > lastBuildDate) {
                    return content != null ? content : new File(geoipdb.getPath());
                } else {
                    return null;
                }
            } finally {
                temproraryReader.close();
            }
        } else {
            logger.debug("No update needed for {}", geoipdb);
            return null;
        }
    }

    public String[] getTypes() {
        return Arrays.stream(types).map(LocationType::name).toArray(String[]::new);
    }

}
