package loghub.processors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import java.util.concurrent.atomic.AtomicReference;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.logging.log4j.Level;

import com.maxmind.db.CacheKey;
import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.DecodedValue;
import com.maxmind.db.NodeCache;
import com.maxmind.db.NodeCache.Loader;
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
import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.CacheManager;
import loghub.configuration.CacheManager.Policy;
import loghub.configuration.Properties;
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
        public Geoip2 build() {
            return new Geoip2(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private URI geoipdb;
    private final LocationType[] types;
    @Getter
    private final String locale;
    @Getter
    private final int cacheSize;
    @Getter
    private final AtomicReference<Reader> reader;
    @Getter
    private Duration delay;

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
                               .map(Helpers::FileUri)
                               .orElse(null);
        this.locale = Locale.forLanguageTag(builder.locale).getLanguage();
        this.cacheSize = builder.cacheSize;
        if (!builder.refresh.trim().isEmpty()) {
            this.delay = Duration.parse(builder.refresh);
        } else {
            this.delay = null;
        }
        this.reader = new AtomicReference<>(null);
    }

    private <T> DatabaseRecord<T> resolveIp(Reader lreader, InetAddress ip, Class<T> clazz) {
        try {
            return lreader.getRecord(ip, clazz);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        // A local copy of the reader, to ensure a consistent usage
        Reader lreader = reader.get();
        if (lreader == null) {
            return RUNSTATUS.FAILED;
        }
        InetAddress ipInfo;
        if (addr instanceof InetAddress) {
            ipInfo = (InetAddress) addr;
        } else if (addr instanceof String) {
            try {
                ipInfo = Helpers.parseIpAddres((String) addr);
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
            switch(lreader.getMetadata().getDatabaseType()) {
            case "GeoIP2-City":
            case "GeoLite2-City": {
                CityResponse response = Optional.of(lreader)
                                                .map(r -> resolveIp(r, ipInfo, CityResponse.class))
                                                .map(DatabaseRecord::getData)
                                                .orElse(null);
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
                CountryResponse response = Optional.of(lreader)
                                                   .map(r -> resolveIp(r, ipInfo, CountryResponse.class))
                                                   .map(DatabaseRecord::getData)
                                                   .orElse(null);
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.getCountry();
                continent = response.getContinent();
                registredCountry = response.getRegisteredCountry();
                representedCountry = response.getRepresentedCountry();
                break;
            }
            case "GeoIP2-ISP":
            default:
                return Optional.of(lreader)
                               .map(r -> resolveIp(r, ipInfo, Map.class))
                               .map(DatabaseRecord::getData)
                               .orElse(null);
            }
        } catch (UncheckedIOException e) {
            throw event.buildException("Can't read GeoIP database", e);
        }

        for (LocationType type: types) {
            switch(type) {
            case COUNTRY:
                if (country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(country.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(country.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if(infos.size() > 0) {
                        informations.put("country", infos);
                    }
                }
                break;
            case REPRESENTEDCOUNTRY:
                if (representedCountry != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(representedCountry.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(representedCountry.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if(infos.size() > 0) {
                        informations.put("represented_country", infos);
                    }
                }
                break;
            case REGISTREDCOUNTRY:
                if (registredCountry != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(registredCountry.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(registredCountry.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if(infos.size() > 0) {
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
                    if(infos.size() > 0) {
                        informations.put("location", infos);
                    }
                }
                break;
            case CONTINENT:
                if(continent != null) {
                    Optional.ofNullable(continent.getNames().get(locale)).ifPresent(i -> informations.put("continent", i));
                }
                break;
            case POSTAL:
                if(postal != null) {
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
                        if (subdivisioninfo.size() > 0) {
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
        @SuppressWarnings("rawtypes")
        CacheManager.Builder<CacheKey, DecodedValue> cacheBuilder = properties.cacheManager.getBuilder(CacheKey.class, DecodedValue.class)
                .setCacheSize(cacheSize)
                .setName("Geoip2", this)
                .setExpiry(Policy.ETERNAL);
        // Kept for compatibility
        if (geoipdb == null) {
            geoipdb = Optional.ofNullable(properties.get("geoip2data"))
                              .map(Object::toString)
                              .map(Helpers::FileUri)
                              .orElse(null);
        }
        refresh(cacheBuilder);
        if (delay != null) {
            properties.registerScheduledTask("refreshgeoip", () -> refresh(cacheBuilder), delay.toMillis());
        }
        return super.configure(properties);
    }

    private void refresh(CacheManager.Builder<CacheKey, DecodedValue> builder) {
        Cache<CacheKey, DecodedValue> cache = builder.build();
        NodeCache nc = (k, l) -> cache.invoke(k, this::extractValue, l);
        Reader lreader = null;
        if (geoipdb != null && "file".equals(geoipdb.getScheme())) {
            try {
                lreader = new Reader(new File(geoipdb.getPath()), nc);
            } catch (IOException e) {
                logger.error("can't read geoip database {}", geoipdb);
                logger.throwing(Level.DEBUG, e);
            }
        } else if (geoipdb != null) {
            try (InputStream is = geoipdb.toURL().openStream()){
                lreader = new Reader(is, nc);
            } catch (IOException e) {
                logger.error("can't read geoip database {}", geoipdb);
                logger.throwing(Level.DEBUG, e);
            }
        } else {
            try {
                try (InputStream is = Geoip2.class.getResourceAsStream("GeoLite2-City.mmdb")) {
                    if (is == null) {
                        logger.error("Didn't find a default database");
                    } else {
                        lreader = new Reader(is, nc);
                    }
                }
            } catch (IOException e) {
                logger.error("Didn't find a default database");
                logger.throwing(Level.DEBUG, e);
            }
        }
        if (lreader != null) {
            reader.set(lreader);
        }
    }

    @SuppressWarnings("rawtypes")
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

    public String[] getTypes() {
        return Arrays.stream(types).map(LocationType::name).toArray(String[]::new);
    }

}
