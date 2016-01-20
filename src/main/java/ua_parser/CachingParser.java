package ua_parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import loghub.configuration.Properties;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * When doing webanalytics (with for example PIG) the main pattern is to process
 * weblogs in clickstreams. A basic fact about common clickstreams is that in
 * general the same browser will do multiple requests in sequence. This has the
 * effect that the same useragent will appear in the logfiles and we will see
 * the need to parse the same useragent over and over again.
 *
 * This class introduces a very simple LRU cache to reduce the number of times
 * the parsing is actually done.
 *
 * @author Niels Basjes
 *
 */
public class CachingParser extends Parser {

    private final Cache    cacheClient;
    private final Cache cacheUserAgent;
    private final Cache    cacheDevice;
    private final Cache        cacheOS;

    // ------------------------------------------

    public CachingParser(int cacheSize, Properties props) throws IOException {
        super();
        cacheClient = props.getCache(cacheSize, "UA-client", this);
        cacheUserAgent = props.getCache(cacheSize, "UA-useragent", this);
        cacheDevice = props.getCache(cacheSize, "UA-device", this);
        cacheOS = props.getCache(cacheSize, "UA-os", this);
    }

    public CachingParser(int cacheSize, Properties props, InputStream regexYaml) {
        super(regexYaml);
        cacheClient = props.getCache(cacheSize, "UA-client", this);
        cacheUserAgent = props.getCache(cacheSize, "UA-useragent", this);
        cacheDevice = props.getCache(cacheSize, "UA-device", this);
        cacheOS = props.getCache(cacheSize, "UA-os", this);
    }

    @SuppressWarnings("unchecked")
    private <T> T getObject(Cache cache, String key, Function<String, T> resolve) {
        T value = null;
        Element e = cache.get(key);
        if(e == null) {
            value = resolve.apply(key);
            cache.put(new Element(key, value));
        } else {
            value = (T) e.getObjectValue();
        }
        return value;
    }

    @Override
    public Client parse(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheClient, agentString, i -> super.parse(agentString));
        }
    }

    @Override
    public UserAgent parseUserAgent(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheUserAgent, agentString, i -> super.parseUserAgent(agentString));
        }
    }

    @Override
    public Device parseDevice(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheDevice, agentString, i -> super.parseDevice(agentString));
        }
    }

    @Override
    public OS parseOS(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheOS, agentString, i -> super.parseOS(agentString));
        }
    }

}
