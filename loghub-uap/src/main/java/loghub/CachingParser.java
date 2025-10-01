package loghub;

import java.io.InputStream;
import java.util.function.Function;

import javax.cache.Cache;

import loghub.configuration.Properties;
import ua_parser.Client;
import ua_parser.Device;
import ua_parser.OS;
import ua_parser.Parser;
import ua_parser.UserAgent;

/**
 * When doing webanalytics (with for example PIG) the main pattern is to process
 * weblogs in clickstreams. A basic fact about common clickstreams is that in
 * general the same browser will do multiple requests in sequence. This has the
 * effect that the same useragent will appear in the logfiles and we will see
 * the need to parse the same useragent over and over again.
 * <p/>
 * This class introduces a very simple LRU cache to reduce the number of times
 * the parsing is actually done.
 *
 * @author Niels Basjes
 *
 */
public class CachingParser extends Parser {

    private final Cache<String, Client>    cacheClient;
    private final Cache<String, UserAgent> cacheUserAgent;
    private final Cache<String, Device>    cacheDevice;
    private final Cache<String, OS>        cacheOS;

    public CachingParser(int cacheSize, Properties props, InputStream regexYaml) {
        super(regexYaml);
        cacheClient = props.cacheManager.getBuilder(String.class, Client.class).setName("UA-client", this).setCacheSize(cacheSize).build();
        cacheUserAgent = props.cacheManager.getBuilder(String.class, UserAgent.class).setName("UA-useragent", this).setCacheSize(cacheSize).build();
        cacheDevice = props.cacheManager.getBuilder(String.class, Device.class).setName("UA-device", this).setCacheSize(cacheSize).build();
        cacheOS = props.cacheManager.getBuilder(String.class, OS.class).setName("UA-os", this).setCacheSize(cacheSize).build();
    }

    private <T> T getObject(Cache<String, T> cache, String key, Function<String, T> resolve) {
        return cache.invoke(key, (e, a) -> {
            if (e.getValue() == null) {
                T value = resolve.apply(e.getKey());
                e.setValue(value);
            }
            return e.getValue();
        });
    }

    @Override
    public Client parse(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheClient, agentString, super::parse);
        }
    }

    @Override
    public UserAgent parseUserAgent(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheUserAgent, agentString, super::parseUserAgent);
        }
    }

    @Override
    public Device parseDevice(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheDevice, agentString, super::parseDevice);
        }
    }

    @Override
    public OS parseOS(String agentString) {
        if (agentString == null) {
            return null;
        } else {
            return getObject(cacheOS, agentString, super::parseOS);
        }
    }

}
