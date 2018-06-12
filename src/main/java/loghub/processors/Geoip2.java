package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.NodeCache;
import com.maxmind.db.NodeCache.Loader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
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
import loghub.configuration.Properties;

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
    };

    private static DatabaseReader reader;

    private Path datfilepath = null;
    private LocationType[] types = new LocationType[] {};
    private String locale = "en";
    private int cacheSize = 100;

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        InetAddress ipInfo = null;
        if (addr instanceof InetAddress) {
            ipInfo = (InetAddress) addr;
        } else if (addr instanceof String) {
            try {
                ipInfo = Helpers.parseIpAddres((String) addr);
                if(ipInfo == null) {
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
                CityResponse response = reader.city(ipInfo);
                if(response == null) {
                    throw event.buildException("City not found for " + ipInfo.toString());
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
                CountryResponse response = reader.country(ipInfo);
                if(response == null) {
                    throw event.buildException("Country not found for " + ipInfo.toString());
                }
                country = response.getCountry();
                continent = response.getContinent();
                registred_country = response.getRegisteredCountry();
                represented_country = response.getRepresentedCountry();
                break;
            }
            }
        } catch (AddressNotFoundException e) {
            // not an error, just return a failure
            return RUNSTATUS.FAILED;
        } catch (IOException | GeoIp2Exception e) {
            throw event.buildException("can't read geoip database", e);
        }

        for(LocationType type: types) {
            switch(type) {
            case COUNTRY:
                if(country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Helpers.putNotEmpty(infos, "code", country.getIsoCode());
                    Helpers.putNotEmpty(infos, "name", country.getNames().get(locale));
                    if(infos.size() > 0) {
                        informations.put("country", infos);
                    }
                }
                break;
            case REPRESENTEDCOUNTRY:
                if(represented_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Helpers.putNotEmpty(infos, "code", represented_country.getIsoCode());
                    Helpers.putNotEmpty(infos, "name", represented_country.getNames().get(locale));
                    if(infos.size() > 0) {
                        informations.put("represented_country", infos);
                    }
                }
                break;
            case REGISTREDCOUNTRY:
                if(registred_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    Helpers.putNotEmpty(infos, "code", registred_country.getIsoCode());
                    Helpers.putNotEmpty(infos, "name", registred_country.getNames().get(locale));
                    if(infos.size() > 0) {
                        informations.put("registred_country", infos);
                    }
                }
                break;
            case CITY: {
                if(city != null) {
                    Helpers.putNotEmpty(informations, "city", city.getNames().get(locale));
                }
                break;
            }
            case LOCATION:
                Map<String, Object> infos = new HashMap<>(10);
                if(location != null) {
                    Helpers.putNotEmpty(infos, "latitude", location.getLatitude());
                    Helpers.putNotEmpty(infos, "longitude", location.getLongitude());
                    Helpers.putNotEmpty(infos, "timezone", location.getTimeZone());
                    Helpers.putNotEmpty(infos, "accuray_radius", location.getAccuracyRadius());
                    Helpers.putNotEmpty(infos, "metro_code", location.getMetroCode());
                    Helpers.putNotEmpty(infos, "average_income", location.getAverageIncome());
                    Helpers.putNotEmpty(infos, "population_density", location.getPopulationDensity());
                    if(infos.size() > 0) {
                        informations.put("location", infos);
                    }
                }
            case CONTINENT:
                if(continent != null) {
                    Helpers.putNotEmpty(informations, "continent", continent.getNames().get(locale));
                }
                break;
            case POSTAL:
                if(postal != null) {
                    Helpers.putNotEmpty(informations, "postal", postal.getCode());
                }
                break;
            case SUBDIVISION:
                if(subdivision != null) {
                    List<Map<String, Object>> all = new ArrayList<>(subdivision.size());
                    for(Subdivision sub: subdivision) {
                        Map<String, Object> subdivisioninfo = new HashMap<>(2);
                        Helpers.putNotEmpty(subdivisioninfo, "code", sub.getIsoCode());
                        Helpers.putNotEmpty(subdivisioninfo, "name", sub.getNames().get(locale));
                        if(subdivisioninfo.size() > 0) {
                            all.add(subdivisioninfo);
                        }
                    }
                    if(all.size() > 0) {
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
        datfilepath = Optional.ofNullable(properties.get("geoip2data")).map(i-> Paths.get(i.toString())).orElse(null);
        if(reader == null) {
            Cache<Integer, JsonNode> ehCache = properties.cacheManager.getBuilder(Integer.class, JsonNode.class)
                            .setCacheSize(cacheSize)
                            .setName("Geoip2", datfilepath != null ? datfilepath : "GeoLite2-City.mmdb")
                            .build();
            EntryProcessor<Integer, JsonNode, JsonNode> ep = (i, j) -> {
                try {
                    if (i.getValue() == null) {
                        Loader loader = (Loader)j[0];
                        JsonNode node = loader.load(i.getKey());
                        i.setValue(node);
                    }
                    return i.getValue();
                } catch (IOException e) {
                    throw new EntryProcessorException(e);
                }
            };
            NodeCache nc = (k, l) -> ehCache.invoke(k, ep, l);

            if(datfilepath != null) {
                try {
                    reader = new DatabaseReader.Builder(datfilepath.toFile()).withCache(nc).build();
                } catch (IOException e) {
                    logger.error("can't read geoip database " + datfilepath.toString());
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
                        reader = new DatabaseReader.Builder(embedded).withCache(nc).build();
                    }
                } catch (IOException e) {
                    logger.error("Didn't find a default database");
                    logger.throwing(Level.DEBUG, e);
                    return false;
                }
            }
        }
        return super.configure(properties);
    }

    public String getDatfilepath() {
        return datfilepath.toString();
    }

    public void setDatfilepath(String datfilepath) {
        this.datfilepath = Paths.get(datfilepath);
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String[] getTypes() {
        return null;
    }

    public void setTypes(String[] types) {
        this.types = new LocationType[types.length];
        for(int i = 0; i < types.length ; i++) {
            this.types[i] = LocationType.valueOf(LocationType.class, types[i].toUpperCase(Locale.ENGLISH));
        }
    }

    /**
     * @return the cacheSize
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * @param cacheSize the cacheSize to set
     */
    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

}
