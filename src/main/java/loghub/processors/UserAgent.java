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

    private Parser uaParser;
    private int cacheSize = 1000;

    @Override
    public boolean processMessage(Event event, String field, String destination) {
        String userAgent = event.get(field).toString();
        Client c = uaParser.parse(userAgent);

        Map<String, Object> ua = new HashMap<>(3);
        Helpers.putNotEmpty(ua, "device", c.device.family);

        Map<String, Object> os =  new HashMap<>(5);
        Helpers.putNotEmpty(os, "family", c.os.family);
        Helpers.putNotEmpty(os, "major", c.os.major);
        Helpers.putNotEmpty(os, "minor", c.os.minor);
        Helpers.putNotEmpty(os, "patch", c.os.patch);
        Helpers.putNotEmpty(os, "patchMinor", c.os.patchMinor);
        if(os.size() > 0) {
            ua.put("os", os);
        }

        Map<String, Object> agent =  new HashMap<>(4);
        Helpers.putNotEmpty(agent, "family", c.userAgent.family);
        Helpers.putNotEmpty(agent, "major", c.userAgent.major);
        Helpers.putNotEmpty(agent, "minor", c.userAgent.minor);
        Helpers.putNotEmpty(agent, "patch", c.userAgent.patch);
        if(agent.size() > 0) {
            ua.put("userAgent", agent);
        }

        event.put(destination, ua);
        
        return true;
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
