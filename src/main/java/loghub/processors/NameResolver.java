package loghub.processors;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.xbill.DNS.Cache;
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.Record;
import org.xbill.DNS.ReverseMap;
import org.xbill.DNS.Type;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import sun.net.util.IPAddressUtil;

@SuppressWarnings("restriction")
public class NameResolver extends FieldsProcessor {

    private String[] resolversNames = null;
    private int timeout = 1;
    private int retries = 2;
    private int cacheSize = 100;
    private ExtendedResolver resolver;
    private Cache cache;

    @Override
    public boolean configure(Properties properties) {
        try {
            if(resolversNames == null) {
                resolver = new ExtendedResolver();
            } else {
                resolver = new ExtendedResolver(resolversNames);
            }
            resolver.setTimeout(timeout);
            resolver.setRetries(retries);
            Cache cache = new Cache();
            cache.setMaxEntries(cacheSize);
        } catch (UnknownHostException ex) {
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public void processMessage(Event event, String field, String destination) throws ProcessorException {
        Object addr = event.get(field);
        InetAddress ip = null;

        // If a string was given, convert it to a Inet?Address
        if(addr instanceof String) {
            String ipstring = (String) addr;
            byte[] parts = null;
            if(IPAddressUtil.isIPv4LiteralAddress(ipstring)) {
                parts = IPAddressUtil.textToNumericFormatV4(ipstring);
            } else if(IPAddressUtil.isIPv6LiteralAddress(ipstring)) {
                parts = IPAddressUtil.textToNumericFormatV6(ipstring);
            }
            if(parts != null) {
                try {
                    ip = InetAddress.getByAddress(parts);
                } catch (UnknownHostException e) {
                    throw new ProcessorException("Invalid host to resolve " + addr, e);
                }
            }
        } else if(addr instanceof InetAddress) {
            ip = (InetAddress) addr;
        } else {
            throw new ProcessorException("Invalid host to resolve " + addr, null);
        }

        Name name = ReverseMap.fromAddress(ip.getAddress());
        Lookup l = new Lookup(name, Type.PTR);
        l.setCache(cache);
        l.setResolver(resolver);
        Record [] records = l.run();
        if (records == null) {
            throw new ProcessorException("unresolvable name " + addr, new UnknownHostException(addr.toString()));
        }
        String value = ((PTRRecord) records[0]).getTarget().toString();
        // Remove the final .
        value = value.substring(0, value.length() - 1);
        event.put(destination, value);
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the url
     */
    public String[] getResolvers() {
        return resolversNames;
    }

    /**
     * @param url the url to set
     */
    public void setResolvers(String[] resolvers) {
        this.resolversNames = resolvers;
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
