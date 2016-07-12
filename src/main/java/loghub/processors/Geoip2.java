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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.NodeCache;
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
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

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

    private static final Logger logger = LogManager.getLogger();
    private static DatabaseReader reader;

    private Path datfilepath = null;
    private LocationType[] types = new LocationType[] {};
    private String locale = "en";

    @Override
    public void processMessage(Event event, String field, String destination) throws ProcessorException {
        Object addr = event.get(field);
        InetAddress ipInfo = null;
        if(addr instanceof InetAddress) {
            ipInfo = (InetAddress) addr;
        } else if (addr instanceof String) {
            try {
                ipInfo = Helpers.parseIpAddres((String) addr);
                if(ipInfo == null) {
                    throw event.buildException("can't read ip address " + addr);
                }
            } catch (UnknownHostException e) {
                throw event.buildException("can't read ip address " + addr, e);
            }
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
            // not an error, do nothing
        } catch (IOException | GeoIp2Exception e) {
            throw event.buildException("can't read geoip database", e);
        }

        Helpers.TriConsumer<String, Object, Map<String, Object>> resolve = (a,b,c) -> { if (b != null) {c.put(a, b);};};

        for(LocationType type: types) {
            switch(type) {
            case COUNTRY:
                if(country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    resolve.accept("code", country.getIsoCode(), infos);
                    resolve.accept("name", country.getNames().get(locale), infos);
                    if(infos.size() > 0) {
                        informations.put("country", infos);
                    }
                }
                break;
            case REPRESENTEDCOUNTRY:
                if(represented_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    resolve.accept("code", represented_country.getIsoCode(), infos);
                    resolve.accept("name", represented_country.getNames().get(locale), infos);
                    if(infos.size() > 0) {
                        informations.put("represented_country", infos);
                    }
                }
                break;
            case REGISTREDCOUNTRY:
                if(registred_country != null) {
                    Map<String, Object> infos = new HashMap<>(2);
                    resolve.accept("code", registred_country.getIsoCode(), infos);
                    resolve.accept("name", registred_country.getNames().get(locale), infos);
                    if(infos.size() > 0) {
                        informations.put("registred_country", infos);
                    }
                }
                break;
            case CITY: {
                if(city != null) {
                    resolve.accept("city", city.getNames().get(locale), informations);
                }
                break;
            }
            case LOCATION:
                Map<String, Object> infos = new HashMap<>(10);
                if(location != null) {
                    resolve.accept("latitude", location.getLatitude(), infos);
                    resolve.accept("longitude", location.getLongitude(), infos);
                    resolve.accept("timezone", location.getTimeZone(), infos);
                    resolve.accept("accuray_radius", location.getAccuracyRadius(), infos);
                    resolve.accept("metro_code", location.getMetroCode(), infos);
                    resolve.accept("average_income", location.getAverageIncome(), infos);
                    resolve.accept("population_density", location.getPopulationDensity(), infos);
                    if(infos.size() > 0) {
                        informations.put("location", infos);
                    }
                }
            case CONTINENT:
                if(continent != null) {
                    resolve.accept("continent", continent.getNames().get(locale), informations);
                }
                break;
            case POSTAL:
                if(postal != null) {
                    resolve.accept("postal", postal.getCode(), informations);
                }
                break;
            case SUBDIVISION:
                if(subdivision != null) {
                    List<Map<String, Object>> all = new ArrayList<>(subdivision.size());
                    for(Subdivision sub: subdivision) {
                        Map<String, Object> subdivisioninfo = new HashMap<>(2);
                        resolve.accept("code", sub.getIsoCode(), subdivisioninfo);
                        resolve.accept("name", sub.getNames().get(locale), subdivisioninfo);
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
        event.put(destination, informations);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean configure(Properties properties) {
        Object datfile = properties.get("geoip2data");
        if(datfile != null) {
            datfilepath = Paths.get(datfile.toString());
        }
        if(reader == null) {
            final Cache ehCache = properties.getCache(100, "Geoip2", this);
            NodeCache nc = new NodeCache() {
                @Override
                public JsonNode get(int key, Loader loader) throws IOException {
                    Element cacheElement = ehCache.get(key);

                    if(cacheElement != null) {
                        return (JsonNode) cacheElement.getObjectValue();
                    } else {
                        JsonNode node = loader.load(key);
                        ehCache.put(new Element(key, node));
                        return node;
                    }
                }
            };
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
                    InputStream embedded = new BufferedInputStream(properties.classloader.getResourceAsStream("GeoLite2-City.mmdb"));
                    reader = new DatabaseReader.Builder(embedded).withCache(nc).build();
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

}
