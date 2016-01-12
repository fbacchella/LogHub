package ua_parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.map.LRUMap;

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
 *
 * This class introduces a very simple LRU cache to reduce the number of times
 * the parsing is actually done.
 *
 * @author Niels Basjes
 *
 */
public class CachingParser extends Parser {

    private final Map<String, Client>    cacheClient;
    private final Map<String, UserAgent> cacheUserAgent;
    private final Map<String, Device>    cacheDevice;
    private final Map<String, OS>        cacheOS;

    // ------------------------------------------

    public CachingParser(int cacheSize) throws IOException {
        super();
        cacheClient = Collections.synchronizedMap(new LRUMap<>(0, cacheSize));
        cacheUserAgent = Collections.synchronizedMap(new LRUMap<>(0, cacheSize));
        cacheDevice = Collections.synchronizedMap(new LRUMap<>(0, cacheSize));
        cacheOS = Collections.synchronizedMap(new LRUMap<>(0, cacheSize));
    }

    public CachingParser(int cacheSize, InputStream regexYaml) {
        super(regexYaml);
        cacheClient = Collections.synchronizedMap(new LRUMap<>(cacheSize));
        cacheUserAgent = Collections.synchronizedMap(new LRUMap<>(cacheSize));
        cacheDevice = Collections.synchronizedMap(new LRUMap<>(cacheSize));
        cacheOS = Collections.synchronizedMap(new LRUMap<>(cacheSize));
    }

    // ------------------------------------------

    @Override
    public Client parse(String agentString) {
        if (agentString == null) {
            return null;
        }
        Client client = cacheClient.get(agentString);
        if (client != null) {
            return client;
        }
        client = super.parse(agentString);
        cacheClient.put(agentString, client);
        return client;
    }

    // ------------------------------------------

    @Override
    public UserAgent parseUserAgent(String agentString) {
        if (agentString == null) {
            return null;
        }
        UserAgent userAgent = cacheUserAgent.get(agentString);
        if (userAgent != null) {
            return userAgent;
        }
        userAgent = super.parseUserAgent(agentString);
        cacheUserAgent.put(agentString, userAgent);
        return userAgent;
    }

    // ------------------------------------------

    @Override
    public Device parseDevice(String agentString) {
        if (agentString == null) {
            return null;
        }
        Device device = cacheDevice.get(agentString);
        if (device != null) {
            return device;
        }
        device = super.parseDevice(agentString);
        cacheDevice.put(agentString, device);
        return device;
    }

    // ------------------------------------------

    @Override
    public OS parseOS(String agentString) {
        if (agentString == null) {
            return null;
        }
        OS os = cacheOS.get(agentString);
        if (os != null) {
            return os;
        }
        os = super.parseOS(agentString);
        cacheOS.put(agentString, os);
        return os;
    }

    // ------------------------------------------

}
