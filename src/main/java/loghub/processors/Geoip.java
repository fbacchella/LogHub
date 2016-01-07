package loghub.processors;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.maxmind.geoip.Country;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

import loghub.Event;
import loghub.Processor;
import loghub.configuration.Properties;

public class Geoip extends Processor {

    private static final Logger logger = LogManager.getLogger();
    static LookupService lookup = null;

    private Path datfilepath = Paths.get("usr","local","share","GeoIP", "GeoIP.dat");
    private String hostfield = "host";
    private String asfield = null;
    private String locationfield = null;
    private String countryfield = null;
    private String countrycodefield = null;
    private String countrynamefield = null;

    @Override
    public void process(Event event) {
        try {
            if(! event.containsKey(hostfield) || event.get(hostfield).toString().isEmpty()) {
                return;
            }
            String host = event.get(hostfield).toString();
            if(host != null) {
                if(asfield != null) {
                    addElement(event, asfield, lookup.getOrg(host));
                }
                if(countryfield != null || countrynamefield != null || countrycodefield != null) {
                    Country c = lookup.getCountry(host);
                    if(countryfield != null) {
                        addElement(event, countryfield, c);
                    }
                    if(countrynamefield != null) {
                        addElement(event, countrynamefield, c.getName());
                    }
                    if(countrycodefield != null) {
                        addElement(event, countrycodefield, c.getCode());
                    }
                }
                if(locationfield != null) {
                    Location l = lookup.getLocation(host);
                    Map<String, Object> lm = new HashMap<>(10);
                    Function.identity();
                    BiConsumer<String, Object> resolve = (a,b) -> { if (b != null) {lm.put(a, b);};};
                    resolve.accept("area_code", l.area_code);
                    resolve.accept("city", l.city);
                    resolve.accept("countryCode", l.countryCode);
                    resolve.accept("countryName", l.countryName);
                    resolve.accept("dma_code", l.dma_code);
                    resolve.accept("latitude", l.latitude);
                    resolve.accept("longitude", l.longitude);
                    resolve.accept("metro_code", l.metro_code);
                    resolve.accept("postalCode", l.postalCode);
                    resolve.accept("region", l.region);
                    addElement(event, locationfield, l);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.catching(e);
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean configure(Properties properties) {
        String datfile = properties.get("geoipdata").toString();
        if(datfile != null) {
            datfilepath = Paths.get(datfile);
        }
        if(lookup == null) {
            try {
                lookup = new LookupService(datfilepath.toString(), LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
            } catch (IOException e) {
                logger.error("Unable to read datfile {}: {}", properties.get("geoipdata"), e.getMessage());
                logger.catching(Level.DEBUG, e);
                return false;
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

    public String getHostfield() {
        return hostfield;
    }

    public void setHostfield(String hostfield) {
        this.hostfield = hostfield;
    }


    public String getAsfield() {
        return asfield;
    }

    public void setAsfield(String asfield) {
        this.asfield = asfield;
    }

    public String getLocationfield() {
        return locationfield;
    }

    public void setLocationfield(String locationfield) {
        this.locationfield = locationfield;
    }

    public String getCountrycodefield() {
        return countrycodefield;
    }

    public void setCountrycodefield(String countrycodefield) {
        this.countrycodefield = countrycodefield;
    }

    public String getCountrynamefield() {
        return countrynamefield;
    }

    public void setCountrynamefield(String countrynamefield) {
        this.countrynamefield = countrynamefield;
    }

    /**
     * @return the countryfield
     */
    public String getCountryfield() {
        return countryfield;
    }

    /**
     * @param countryfield the countryfield to set
     */
    public void setCountryfield(String countryfield) {
        this.countryfield = countryfield;
    }

}
