package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.CacheManager.Policy;
import loghub.configuration.Properties;
import lombok.Getter;
import lombok.Setter;

public class Geoip2 extends FieldsProcessor {

    static enum LocationType {
        COUNTRY,
        REGISTREDCOUNTRY,
        REPRESENTEDCOUNTRY,
        CITY,
        LOCATION,
        CONTINENT,
        POSTAL,
        SUBDIVISION,
    }


    private Path geoipdb = null;
    private LocationType[] types = new LocationType[] {};
    @Getter @Setter
    private String locale = null;
    @Getter @Setter
    private int cacheSize = 100;
    private Reader reader;

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        InetAddress ipInfo = null;
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

        Country country = null;
        Country registred_country = null;
        Country represented_country = null;
        City city = null;
        Continent continent = null;
        Location location = null;
        Postal postal = null;
        List<Subdivision> subdivision = null;

        Map<String, Object> informations = new HashMap<>();

        try {
            switch(reader.getMetadata().getDatabaseType()) {
            case "GeoIP2-City":
            case "GeoLite2-City": {
                CityResponse response = Optional.ofNullable(reader.getRecord(ipInfo, CityResponse.class)).map(DatabaseRecord::getData).orElse(null);
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.getCountry();
                city = response.getCity();
                continent = response.getContinent();
                location = response.getLocation();
                postal = response.getPostal();
                registred_country = response.getRegisteredCountry();
                represented_country = response.getRepresentedCountry();
                subdivision = response.getSubdivisions();
                break;
            }
            case "GeoIP2-Country":
            case "GeoLite2-Country": {
                CountryResponse response = Optional.ofNullable(reader.getRecord(ipInfo, CountryResponse.class)).map(DatabaseRecord::getData).orElse(null);
                if (response == null) {
                    return RUNSTATUS.FAILED;
                }
                country = response.getCountry();
                continent = response.getContinent();
                registred_country = response.getRegisteredCountry();
                represented_country = response.getRepresentedCountry();
                break;
            }
            case "GeoIP2-ISP":
            default:
                return Optional.ofNullable(reader.getRecord(ipInfo, Map.class)).map(DatabaseRecord::getData).orElse(null);
            }
        } catch (IOException e) {
            throw event.buildException("Can't read GeoIP database", e);
        }

        for(LocationType type: types) {
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
                if (represented_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(represented_country.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(represented_country.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
                    if(infos.size() > 0) {
                        informations.put("represented_country", infos);
                    }
                }
                break;
            case REGISTREDCOUNTRY:
                if (registred_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Optional.ofNullable(registred_country.getIsoCode()).ifPresent(i -> infos.put("code", i));
                    Optional.ofNullable(registred_country.getNames().get(locale)).ifPresent(i -> infos.put("name", i));
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
                    if (all.size() > 0) {
                        informations.put("subdivisions", all);
                    }
                }
                break;
            }
        }
        if (informations.size() > 0) {
            return informations;
        } else {
            return RUNSTATUS.FAILED;
        }
    }

    @Override
    public boolean configure(Properties properties) {
        // It might have been setup by properties
        if (locale == null) {
            locale = Locale.getDefault().getCountry();
        }
        // Kept for compatibility
        if (geoipdb == null) {
            geoipdb = Optional.ofNullable(properties.get("geoip2data")).map(i-> Paths.get(i.toString())).orElse(null);
        }

        @SuppressWarnings("rawtypes")
        Cache<CacheKey, DecodedValue> cache = properties.cacheManager.getBuilder(CacheKey.class, DecodedValue.class)
                        .setCacheSize(cacheSize)
                        .setName("Geoip2", this)
                        .setExpiry(Policy.ETERNAL)
                        .build();
        NodeCache nc = (k, l) -> cache.invoke(k, this::extractValue, l);
        if (geoipdb != null) {
            try {
                reader = new Reader(geoipdb.toFile(), nc);
            } catch (IOException e) {
                logger.error("can't read geoip database " + geoipdb.toString());
                logger.throwing(Level.DEBUG, e);
                return false;
            }
        } else {
            try {
                InputStream is = properties.classloader.getResourceAsStream("GeoLite2-City.mmdb");
                if (is == null) {
                    logger.error("Didn't find a default database");
                    return false;
                } else {
                    InputStream embedded = new BufferedInputStream(is);
                    reader = new Reader(embedded, nc);
                }
            } catch (IOException e) {
                logger.error("Didn't find a default database");
                logger.throwing(Level.DEBUG, e);
                return false;
            }
        }
        return super.configure(properties);
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

    public String getGeoipdb() {
        return geoipdb.toString();
    }

    public void setGeoipdb(String datfilepath) {
        this.geoipdb = Paths.get(datfilepath);
    }

    public String[] getTypes() {
        return Arrays.stream(types).map(LocationType::name).toArray(String[]::new);
    }

    public void setTypes(String[] types) {
        this.types = new LocationType[types.length];
        for(int i = 0; i < types.length ; i++) {
            this.types[i] = LocationType.valueOf(types[i].toUpperCase(Locale.ENGLISH));
        }
    }

}
