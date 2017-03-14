package loghub.processors;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
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
    private String agentsFile = null;
    private URL agentsUrl = null;

    @Override
    public boolean processMessage(Event event, String field, String destination) {
        if (event.get(field) == null) {
            return false;
        }
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
        InputStream is;
        if (agentsUrl != null) {
            try {
                is = agentsUrl.openStream();
            } catch (IOException e) {
                logger.error("URL {} can't be used as user-agent regexes source: {}", agentsUrl, e.getMessage());
                return false;
            }
        } else if (agentsFile != null && ! agentsFile.isEmpty()) {
            try {
                is = new FileInputStream(agentsFile);
            } catch (FileNotFoundException e) {
                logger.error("File {} can't be used as user-agent regexes source: {}", agentsFile, e.getMessage());
                return false;
            }
        } else {
            is = properties.classloader.getResourceAsStream("ua_parser/regexes.yaml");
            if(is == null) {
                logger.error("ua_parser/regexes.yaml is not in the classpath or can't be loaded");
                return false;
            }
        }
        is = new BufferedInputStream(is);
        uaParser = new CachingParser(cacheSize, properties, is);
        try {
            is.close();
        } catch (IOException e) {
            logger.error("close failure");
            return false;
        }
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

    /**
     * @return the agentsFile
     */
    public String getAgentsFile() {
        return agentsFile == null ? "" : agentsFile;
    }

    /**
     * @param agentsFile the agentsFile to set
     */
    public void setAgentsFile(String agentsFile) {
        this.agentsFile = agentsFile;
    }

    /**
     * @return the agentsUrl
     */
    public String getAgentsUrl() {
        return agentsUrl == null ? "" : agentsUrl.toString();
    }

    /**
     * @param agentsUrl An URL where the regexes file can be found (like the master file https://raw.githubusercontent.com/ua-parser/uap-core/master/regexes.yaml)
     */
    public void setAgentsUrl(String agentsUrl) {
        try {
            this.agentsUrl = new URL(agentsUrl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("can't parse URL " + agentsUrl, e);
        }
    }

}
