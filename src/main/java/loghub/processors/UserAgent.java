package loghub.processors;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.Parser;

public class UserAgent extends FieldsProcessor {

    private final static Helpers.TriConsumer<String, Object, Map<String, Object>> smartput = (a,b,c) -> { if (b != null) {c.put(a, b);};};

    private Parser uaParser;
    private int cacheSize = 1000;

    @Override
    public void processMessage(Event event, String field, String destination) {
        String userAgent = event.get(field).toString();
        Client c = uaParser.parse(userAgent);

        Map<String, Object> ua = new HashMap<>(3);
        smartput.accept("device", c.device.family, ua);

        Map<String, Object> os =  new HashMap<>(5);
        smartput.accept("family", c.os.family, os);
        smartput.accept("major", c.os.major, os);
        smartput.accept("minor", c.os.minor, os);
        smartput.accept("patch", c.os.patch, os);
        smartput.accept("patchMinor", c.os.patchMinor, os);
        if(os.size() > 0) {
            ua.put("os", os);
        }

        Map<String, Object> agent =  new HashMap<>(4);
        smartput.accept("family", c.userAgent.family, agent);
        smartput.accept("major", c.userAgent.major, agent);
        smartput.accept("minor", c.userAgent.minor, agent);
        smartput.accept("patch", c.userAgent.patch, agent);
        if(agent.size() > 0) {
            ua.put("userAgent", agent);
        }

        event.put(destination, ua);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean configure(Properties properties) {
        InputStream is = properties.classloader.getResourceAsStream("ua_parser/regexes.yaml");
        if(is == null) {
            return false;
        }
        is = new BufferedInputStream(is);
        uaParser = new CachingParser(cacheSize, properties, is);

        return super.configure(properties);
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
