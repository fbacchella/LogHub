package loghub.processors;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import loghub.Event;
import loghub.configuration.Properties;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.Parser;

public class UserAgent extends FieldsProcessor {

    private Parser uaParser;
    private int cacheSize = 1000;

    @Override
    public void processMessage(Event event, String field) {
        String userAgent = event.get(field).toString();
        Client c = uaParser.parse(userAgent);
        Map<String, Object> ua = new HashMap<>(3);
        ua.put("device", c.device.family);
        
        Map<String, Object> os =  new HashMap<>(5);
        os.put("family", c.os.family);
        os.put("major", c.os.major);
        os.put("minor", c.os.minor);
        os.put("patch", c.os.patch);
        os.put("patchMinor", c.os.patchMinor);
        ua.put("os", os);
        
        Map<String, Object> agent =  new HashMap<>(4);
        agent.put("family", c.userAgent.family);
        agent.put("major", c.userAgent.major);
        agent.put("minor", c.userAgent.minor);
        agent.put("patch", c.userAgent.patch);
        ua.put("userAgent", agent);
        
        event.put(field, ua);
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
        uaParser = new CachingParser(cacheSize, is);

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
