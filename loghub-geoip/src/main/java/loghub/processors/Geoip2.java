package loghub.processors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.maxmind.db.CacheKey;
import com.maxmind.db.DecodedValue;
import com.maxmind.db.NodeCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.model.AnonymousPlusResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.model.IpRiskResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Traits;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.CacheManager;
import loghub.configuration.CacheManager.Policy;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.httpclient.JavaHttpClientService;
import loghub.jackson.JacksonBuilder;
import loghub.processors.Geoip2.Builder;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Builder.class)
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
        ISP,
        TRAITS
    }

    private static final ObjectMapper mapper = JacksonBuilder.get(JsonMapper.class).getMapper();

    private record LogHubNodeCache(Cache<CacheKey, DecodedValue> cache) implements NodeCache {
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
                    if (!i.exists()) {
                        Loader loader = (Loader) j[0];
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

    @FunctionalInterface
    interface MakeReader {
        Reader build(NodeCache cache) throws IOException;
    }

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Geoip2> {
        private String geoipdb = null;
        private String[] types = new String[] {};
        private String locale = Locale.getDefault().getCountry();
        private int cacheSize = 100;
        private String refresh = "";
        private CacheManager cacheManager;
        private boolean required = true;
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
    private final boolean required;
    @Getter
    private final Duration delay;
    private final ReadWriteLock dbProtectionLock = new ReentrantReadWriteLock();
    private volatile long lastBuildDate = 0;

    public Geoip2(Builder builder) {
        super(builder);
        if (builder.types.length == 0) {
            this.types = LocationType.values();
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
        Cache<CacheKey, DecodedValue> cache = builder.cacheManager
                                                     .getBuilder(CacheKey.class, DecodedValue.class)
                                                     .setCacheSize(builder.cacheSize)
                                                     .setName("Geoip2", this)
                                                     .setExpiry(Policy.ETERNAL)
                                                     .build();
        this.geoipCache = new LogHubNodeCache(cache);
        this.required = builder.required;
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
        switch (addr) {
        case InetAddress inetAddres -> ipInfo = inetAddres;
        case String s -> {
            try {
                ipInfo = Helpers.parseIpAddress(s);
            } catch (UnknownHostException e) {
                throw event.buildException("can't read IP address " + addr, e);
            }
        }
        default -> throw event.buildException("It's not an IP address: " + addr);
        }

        // If reader is null, it started with a failed geoloc db and required is false
        // so just fails the resolution
        if (reader == null) {
            return RUNSTATUS.FAILED;
        }

        Country country = null;
        Country registredCountry = null;
        RepresentedCountry representedCountry = null;
        City city = null;
        Continent continent = null;
        Location location = null;
        Postal postal = null;
        List<Subdivision> subdivision = null;
        Traits traits = null;
        Map<?, ?> isp = null;
        Map<?, ?> anonymous = null;
        Map<?, ?> ipRisk = null;
        Map<?, ?> connectionType = null;
        Map<?, ?> domain = null;
        Map<?, ?> asn = null;
        Map<?, ?> densityIncome = null;
        double ipScore = Double.NaN;
        Map<?, ?> userCount = null;


        Map<String, Object> informations = new HashMap<>();
        try {
            switch (reader.getMetadata().databaseType()) {
            case "GeoIP2-City", "GeoLite2-City", "GeoIP2-City-Shield" -> {
                CityResponse response = reader.getRecord(ipInfo, CityResponse.class).data();
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.country();
                city = response.city();
                continent = response.continent();
                location = response.location();
                postal = response.postal();
                registredCountry = response.registeredCountry();
                representedCountry = response.representedCountry();
                subdivision = response.subdivisions();
                traits = response.traits();
            }
            case "GeoIP2-Country", "GeoLite2-Country", "GeoIP2-Country-Shield" -> {
                CountryResponse response = reader.getRecord(ipInfo, CountryResponse.class).data();
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.country();
                continent = response.continent();
                registredCountry = response.registeredCountry();
                representedCountry = response.representedCountry();
                traits = response.traits();
            }
            case "GeoIP2-Precision-Enterprise", "GeoIP2-Enterprise", "GeoIP2-Enterprise-Shield", "GeoIP2-Precision-Enterprise-Shield" -> {
                EnterpriseResponse response = reader.getRecord(ipInfo, EnterpriseResponse.class).data();
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.country();
                city = response.city();
                continent = response.continent();
                location = response.location();
                postal = response.postal();
                registredCountry = response.registeredCountry();
                representedCountry = response.representedCountry();
                subdivision = response.subdivisions();
                traits = response.traits();
            }
            case "GeoIP-Anonymous-Plus", "GeoIP2-Anonymous-IP" ->
                    anonymous = readRecordAsMap(ipInfo, AnonymousPlusResponse.class);
            case "GeoIP2-Connection-Type" -> connectionType = readRecordAsMap(ipInfo, ConnectionTypeResponse.class);
            case "GeoIP2-ISP" -> isp = readRecordAsMap(ipInfo, IspResponse.class);
            case "GeoIP2-IP-Risk" -> ipRisk = readRecordAsMap(ipInfo, IpRiskResponse.class);
            case "GeoIP2-Domain" -> domain = readRecordAsMap(ipInfo, DomainResponse.class);
            case "GeoLite2-ASN" -> asn = readRecordAsMap(ipInfo, AsnResponse.class);
            case "GeoIP2-DensityIncome" -> densityIncome = reader.getRecord(ipInfo, Map.class).data();
            case "GeoIP2-User-Count" -> userCount = reader.getRecord(ipInfo, Map.class).data();
            case "GeoIP2-Static-IP-Score" -> {
                Map<?, ?> map = reader.getRecord(ipInfo, Map.class).data();
                ipScore = ((Number) map.get("score")).doubleValue();
            }
            default -> {
                Map<?, ?> response = reader.getRecord(ipInfo, Map.class).data();
                System.err.println(response);
                return response == null ? RUNSTATUS.FAILED : response;
            }
            }
        } catch (IOException e) {
            throw event.buildException("Can't read GeoIP database", e);
        }

        for (LocationType type: types) {
            switch(type) {
            case COUNTRY -> {
                if (country != null) {
                    Map<String, Object> infos = HashMap.newHashMap(2);
                    Optional.ofNullable(country.isoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(country.names().get(locale)).ifPresent(i -> infos.put("name", i));
                    if (!infos.isEmpty()) {
                        informations.put("country", infos);
                    }
                }
            }
            case REPRESENTEDCOUNTRY -> {
                if (representedCountry != null) {
                    Map<String, Object> infos = HashMap.newHashMap(2);
                    Optional.ofNullable(representedCountry.isoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(representedCountry.names().get(locale)).ifPresent(i -> infos.put("name", i));
                    Optional.ofNullable(representedCountry.type()).ifPresent(i -> infos.put("type", i));
                    if (!infos.isEmpty()) {
                        informations.put("represented_country", infos);
                    }
                }
            }
            case REGISTREDCOUNTRY -> {
                if (registredCountry != null) {
                    Map<String, Object> infos = HashMap.newHashMap(2);
                    Optional.ofNullable(registredCountry.isoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(registredCountry.names().get(locale)).ifPresent(i -> infos.put("name", i));
                    if (!infos.isEmpty()) {
                        informations.put("registred_country", infos);
                    }
                }
            }
            case CITY -> {
                if (city != null) {
                    Optional.ofNullable(city.names().get(locale)).ifPresent(i -> informations.put("city", i));
                }
            }
            case LOCATION -> {
                Map<String, Object> infos = HashMap.newHashMap(7);
                if (location != null) {
                    Optional.ofNullable(location.latitude()).ifPresent(i -> infos.put("latitude", i));
                    Optional.ofNullable(location.longitude()).ifPresent(i -> infos.put("longitude", i));
                    Optional.ofNullable(location.timeZone()).ifPresent(i -> infos.put("timezone", i));
                    Optional.ofNullable(location.accuracyRadius()).ifPresent(i -> infos.put("accuray_radius", i));
                    Optional.ofNullable(location.averageIncome()).ifPresent(i -> infos.put("average_income", i));
                    Optional.ofNullable(location.populationDensity()).ifPresent(i -> infos.put("population_density", i));
                    if (!infos.isEmpty()) {
                        informations.put("location", infos);
                    }
                }
            }
            case CONTINENT -> {
                if (continent != null) {
                    Optional.ofNullable(continent.names().get(locale)).ifPresent(i -> informations.put("continent", i));
                }
            }
            case POSTAL -> {
                if (postal != null) {
                    Optional.ofNullable(postal.code()).ifPresent(i -> informations.put("postal", i));
                }
            }
            case SUBDIVISION -> {
                if (subdivision != null) {
                    List<Map<String, Object>> all = new ArrayList<>(subdivision.size());
                    for (Subdivision sub: subdivision) {
                        Map<String, Object> subdivisioninfo = HashMap.newHashMap(2);
                        Optional.ofNullable(sub.isoCode()).ifPresent(i -> subdivisioninfo.put("code", i));
                        Optional.ofNullable(sub.names().get(locale)).ifPresent(i -> subdivisioninfo.put("name", i));
                        if (!subdivisioninfo.isEmpty()) {
                            all.add(subdivisioninfo);
                        }
                    }
                    if (!all.isEmpty()) {
                        informations.put("subdivisions", all);
                    }
                }
            }
            case ISP -> {
                if (isp != null && ! isp.isEmpty()) {
                    informations.put("isp", isp);
                }
            }
            case TRAITS -> {
                if (traits != null) {
                    Map<String, Object> infos = filterRecord(traits);
                    informations.put("traits", infos);
                }
            }
            }
        }
        if (! informations.isEmpty()) {
            return informations;
        } else {
            return RUNSTATUS.FAILED;
        }
    }

    private Map<String, Object> filterRecord(Record r) {
        return mapper.convertValue(r, new TypeReference<Map<String, Object>>() {})
                     .entrySet()
                     .stream()
                     .filter(e -> e.getValue() != null)
                     .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private <R extends Record> Map<String, Object> readRecordAsMap(InetAddress addr, Class<R> cls) throws IOException {
        R response = reader.getRecord(addr, cls).data();
        return filterRecord(response);
    }

    @Override
    public boolean configure(Properties properties) {
        refresh();
        if (delay != null) {
            properties.registerScheduledTask("refreshgeoip", this::refresh, delay.toMillis());
        }
        return (reader != null || ! required) && super.configure(properties);
    }

    private void refresh() {
        logger.debug("Will refresh GeoIP database from {}", () -> geoipdb);
        Optional<MakeReader> newContentMaker;
        try {
            newContentMaker = readNewDb();
        } catch (IOException e) {
            logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Unable to read the GeoIP database content: {}", () -> Helpers.resolveThrowableException(e));
            return;
        }
        if (newContentMaker.isPresent()) {
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
                reader = newContentMaker.get().build(geoipCache);
                logger.debug("Reloaded GeoIP database of type {} from {}", () -> reader.getMetadata().databaseType(), () -> geoipdb);
                lastBuildDate = reader.getMetadata().buildTime().toEpochMilli();
            } catch (IOException e) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Unable to read the GeoIP database content: {}", () -> Helpers.resolveThrowableException(e));
            } finally {
                dbProtectionLock.writeLock().unlock();
            }
        }
    }

    private Optional<MakeReader> readNewDb() throws IOException {
        byte[] content;
        Reader temproraryReader;
        if (geoipdb != null && "file".equals(geoipdb.getScheme())) {
            content = null;
            temproraryReader = new Reader(Path.of(geoipdb).toFile());
        } else if (geoipdb != null && geoipdb.getScheme().startsWith("http")) {
            content = readHttp();
            if (content != null) {
                temproraryReader = new Reader(new ByteArrayInputStream(content));
            } else {
                logger.debug("Empty DB content from {}", geoipdb);
                return Optional.empty();
            }
        } else if (geoipdb != null) {
            try (InputStream is = geoipdb.toURL().openStream()) {
                content = is.readAllBytes();
                temproraryReader = new Reader(new ByteArrayInputStream(content));
            }
        } else {
            try (InputStream is = Geoip2.class.getResourceAsStream("GeoLite2-City.mmdb")) {
                if (is == null) {
                    logger.error("Didn't find a default database");
                    return Optional.empty();
                } else {
                    content = is.readAllBytes();
                    temproraryReader = new Reader(new ByteArrayInputStream(content));
                }
            }
        }
        try {
            logger.debug("Comparing new DB time {} with old {}", temproraryReader::getMetadata, () -> Instant.ofEpochMilli(lastBuildDate));
            if (temproraryReader.getMetadata().buildTime().toEpochMilli() > lastBuildDate) {
                MakeReader mr;
                if (content != null) {
                    mr = c -> {
                        try (InputStream is = new ByteArrayInputStream(content)) {
                            return new Reader(is, geoipCache);
                        }
                    };
                } else {
                    mr = c -> new Reader(Path.of(geoipdb).toFile(), c);
                }
                return Optional.of(mr);
            } else {
                return Optional.empty();
            }
        } finally {
            temproraryReader.close();
        }
    }

    private byte[] readHttp() throws IOException {
        JavaHttpClientService.Builder clientBuilder = JavaHttpClientService.getBuilder();
        clientBuilder.setTimeout(5);
        AbstractHttpClientService httpClient = clientBuilder.build();
        HttpRequest<byte[]> request = httpClient.getRequest();
        request.setUri(geoipdb);
        request.setConsumeBytes(InputStream::readAllBytes);
        try (HttpResponse<byte[]> response = httpClient.doRequest(request)) {
            logger.debug("Response status is {} from {}", response::getStatus, () -> geoipdb);
            if (! response.isConnexionFailed()) {
                return response.getParsedResponse();
            } else {
                IOException ex = response.getSocketException();
                if (ex != null) {
                    throw ex;
                } else {
                    logger.error("Failed to download {}: {}", geoipdb, response.getStatus());
                    return null;
                }
            }
        }
    }

    public String[] getTypes() {
        return Arrays.stream(types).map(LocationType::name).toArray(String[]::new);
    }

}
