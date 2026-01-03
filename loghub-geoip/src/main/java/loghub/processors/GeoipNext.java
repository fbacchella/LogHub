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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import com.maxmind.db.CacheKey;
import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.DecodedValue;
import com.maxmind.db.NodeCache;
import com.maxmind.db.Reader;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.CacheManager;
import loghub.configuration.CacheManager.Policy;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.httpclient.JavaHttpClientService;
import loghub.processors.FieldsProcessor.InPlace;
import loghub.processors.GeoipNext.Builder;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Builder.class)
@InPlace
public class GeoipNext extends FieldsProcessor {

    private static final Set<String> ALL = HashSet.newHashSet(0);

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
    public static class Builder extends FieldsProcessor.Builder<GeoipNext> {
        private String geoipdb = null;
        // Keep compatibily with previous version of this processor
        private String isoCodeKey = "code";
        private String[] types = new String[] {};
        private String locale = Locale.getDefault().getLanguage();
        private int cacheSize = 100;
        private String refresh = "";
        private CacheManager cacheManager;
        private boolean required = true;
        private boolean keepOldLayout = true;
        private boolean returnNetwork = false;
        public GeoipNext build() {
            return new GeoipNext(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final URI geoipdb;
    private final Set<String> types;
    @Getter
    private final String locale;
    @Getter
    private volatile Reader reader;
    private final LogHubNodeCache geoipCache;
    private final boolean required;
    @Getter
    private final Duration delay;
    private final String isoCodeKey;
    private final boolean keepOldLayout;
    private final boolean returnNetwok;
    private final ReadWriteLock dbProtectionLock = new ReentrantReadWriteLock();
    private volatile long lastBuildDate = 0;

    public GeoipNext(Builder builder) {
        super(builder);
        if (builder.types.length == 0 && builder.keepOldLayout) {
            this.types = Set.of("country", "continent", "name", "city", "latitude", "longitude", "code", "timezone",
                                "accuray_radius", "metro_code", "average_income", "population_density");
        } else if (builder.types.length == 0) {
            this.types = Set.of("country", "continent", "name", "city", "latitude", "longitude", builder.isoCodeKey);
        } else if (builder.types.length == 1 && builder.types[0].equalsIgnoreCase("all")) {
            this.types = ALL;
        } else {
            this.types = Set.of(builder.types);
        }
        this.keepOldLayout = builder.keepOldLayout;
        this.returnNetwok = builder.returnNetwork;
        this.isoCodeKey = builder.keepOldLayout ? "code" : builder.isoCodeKey;
        this.geoipdb = Optional.ofNullable(builder.geoipdb)
                               .map(Helpers::fileUri)
                               .orElse(null);
        this.locale = builder.locale.equals("posix") ? "en" : Locale.forLanguageTag(builder.locale).getLanguage();
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
        // If reader is null, it started with a failed geoloc db and required is false
        // so just fails the resolution
        if (reader == null) {
            return RUNSTATUS.FAILED;
        }

        InetAddress ipInfo;
        switch (addr) {
        case InetAddress inetAddres -> ipInfo = inetAddres;
        case String s -> {
            try {
                ipInfo = Helpers.parseIpAddress(s);
            } catch (UnknownHostException e) {
                throw event.buildException("Can't read IP address " + addr, e);
            }
        }
        default -> throw event.buildException("It's not an IP address: " + addr);
        }

        DatabaseRecord<Map> dbRecord;
        try {
            dbRecord = reader.getRecord(ipInfo, Map.class);
        } catch (IOException e) {
            throw event.buildException("Can't read GeoIP database", e);
        }
        Map<String, Object> informations = filterMap(dbRecord.data());
        if (returnNetwok && ! informations.containsKey("network")) {
            informations.put("network", dbRecord.network().toString());
        }
        if (informations.isEmpty()) {
            return RUNSTATUS.FAILED;
        } else if (! isInPlace()) {
            return informations;
        } else {
            for (Map.Entry<String, Object> e : informations.entrySet()) {
                VariablePath vp = VariablePath.of(e.getKey());
                event.putAtPath(vp, e.getValue());
            }
            return RUNSTATUS.NOSTORE;
        }
    }

    private Map<String, Object> filterMap(Map<String, Object> map) {
        if (map == null) {
            return HashMap.newHashMap(1);
        } else {
            return map.entrySet()
                      .stream()
                      .map(this::filterRecordEntry)
                      .filter(Optional::isPresent)
                      .map(Optional::get)
                      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    private Optional<Map.Entry<String, Object>> filterRecordEntry(Map.Entry<String, Object> e) {
        if (e.getValue() == null) {
            return Optional.empty();
        } else if (e.getKey().equals("names") && checkKey("name")) {
            @SuppressWarnings("unchecked")
            Map<String, String> langs = (Map<String, String>) e.getValue();
            return Optional.of(Map.entry("name", langs.get(locale)));
        } else if (e.getKey().equals("iso_code") && checkKey(isoCodeKey)){
            return Optional.of(Map.entry(isoCodeKey, e.getValue()));
        } else if (e.getKey().equals("continent") && checkKey("continent") && keepOldLayout) {
            @SuppressWarnings("unchecked")
            Map<String, Object> filtered = filterMap((Map<String, Object>) e.getValue());
            return filtered.containsKey("name") ? Optional.of(Map.entry("continent", filtered.get("name"))) : Optional.empty();
        } else if (e.getKey().equals("postal") && checkKey(isoCodeKey) && keepOldLayout) {
            @SuppressWarnings("unchecked")
            Map<String, Object> filtered = filterMap((Map<String, Object>) e.getValue());
            return filtered.containsKey("code") ? Optional.of(Map.entry("postal", filtered.get("code"))) : Optional.empty();
        } else if (e.getKey().equals("city") && e.getValue() instanceof Map && checkKey(isoCodeKey) && keepOldLayout) {
            @SuppressWarnings("unchecked")
            Map<String, Object> filtered = filterMap((Map<String, Object>) e.getValue());
            return filtered.containsKey("name") ? Optional.of(Map.entry("city", filtered.get("name"))) : Optional.empty();
        } else if (e.getValue() instanceof Map && checkKey(e.getKey())){
            @SuppressWarnings("unchecked")
            Map<String, Object> submap = (Map<String, Object>) e.getValue();
            Map<String, Object> filtered = filterMap(submap);
            return filtered.isEmpty() ? Optional.empty() : Optional.of(Map.entry(e.getKey(), filtered));
        } else if (e.getValue() instanceof List<?> l && checkKey(e.getKey())) {
            String key = e.getKey();
            List<?> filtered = l.stream()
                                .map(i -> filterRecordEntry(Map.entry(key, i)))
                                .filter(Optional::isPresent)
                                .map(o -> o.get().getValue())
                                .toList();
            return filtered.isEmpty() ? Optional.empty() : Optional.of(Map.entry(key, filtered));
        } else if (checkKey(e.getKey())) {
            return Optional.of(e);
        } else {
            return Optional.empty();
        }
    }

    private boolean checkKey(String key) {
        if (types == ALL || (types.contains("all") && ! types.contains("-" + key))) {
            return true;
        } else
            return types.contains(key) && !types.contains("-" + key);
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
            try (InputStream is = GeoipNext.class.getResourceAsStream("GeoLite2-City.mmdb")) {
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
                            return new Reader(is, c);
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
        return types.toArray(String[]::new);
    }

}
