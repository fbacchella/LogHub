package loghub.processors;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;

import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import sun.net.util.IPAddressUtil;

@SuppressWarnings("restriction")
public class NameResolver extends FieldsProcessor {

    private ThreadLocal<DirContext> localContext = new ThreadLocal<DirContext>(){
        @Override
        protected DirContext initialValue() {
            Hashtable<String, String> env = new Hashtable<>();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            env.put("java.naming.provider.url", url);
            env.put("com.example.jndi.dns.timeout.initial", Integer.toString(timeout));
            env.put("com.example.jndi.dns.timeout.retries", Integer.toString(retries));
            try {
                return new InitialDirContext(env);
            } catch (NamingException e) {
                throw new IllegalArgumentException(e);
            }
        }

    };

    private String url = "dns:";
    private String type = "PTR";
    private int timeout = 1;
    private int retries = 2;
    private int cacheSize = 1000;
    private Cache hostCache;
    private int ttl = 60;

    @Override
    public boolean configure(Properties properties) {
        CacheConfiguration config = properties.getDefaultCacheConfig()
                .maxEntriesLocalHeap(cacheSize)
                .timeToLiveSeconds(ttl);

        hostCache = properties.getCache(config);
        return super.configure(properties);
    }

    @Override
    public void processMessage(Event event, String field, String destination) throws ProcessorException {
        Object addr = event.get(field);

        Element cacheElement = hostCache.get(addr.toString());

        if(cacheElement != null && ! cacheElement.isExpired()) {
            Object o = cacheElement.getObjectValue();
            //Set only if it was not a negative cache
            if(o != null) {
                event.put(destination, o);
            }
            return;
        }

        String toresolv = null;

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
                    addr = InetAddress.getByAddress(parts);
                } catch (UnknownHostException e) {
                    throw new ProcessorException("unknonw host " + addr, e);
                }
            }
        }

        if(addr instanceof Inet4Address) {
            Inet4Address ipv4 = (Inet4Address) addr;
            byte[] parts = ipv4.getAddress();
            // the & 0xFF is needed because bytes are signed bytes
            toresolv = String.format("%d.%d.%d.%d.in-addr.arpa.", parts[3] & 0xFF , parts[2] & 0xFF , parts[1] & 0xFF, parts[0] & 0xFF);
        } else if(addr instanceof Inet6Address) {
            Inet6Address ipv6 = (Inet6Address) addr;
            byte[] parts = ipv6.getAddress();
            StringBuilder buffer = new StringBuilder();
            for(int i = parts.length - 1; i >= 0; i--) {
                buffer.append(String.format("%x.%x.", parts[i] & 0x0F, (parts[i] & 0xF0) >> 4));
            }
            buffer.append("ip6.arpa");
            toresolv = buffer.toString();
        }

        //If a query was build, use it
        if (toresolv != null) {
            try {
                Attributes attrs = localContext.get().getAttributes(toresolv, new String[] { type });
                for (NamingEnumeration<? extends Attribute> ae = attrs.getAll(); ae.hasMoreElements();) {
                    Attribute attr = (Attribute) ae.next();
                    if (attr.getID() != type) {
                        continue;
                    }
                    Object o = attr.getAll().next();
                    if (o != null) {
                        String value = attr.getAll().next().toString();
                        value = value.substring(0, value.length() - 1);
                        hostCache.put(new Element(addr.toString(), value));
                        event.put(destination, value);
                    }
                }
            } catch (IllegalArgumentException e) {
                throw new ProcessorException("can't setup resolver " + addr, (Exception) e.getCause());
            } catch (NameNotFoundException | javax.naming.ServiceUnavailableException | javax.naming.CommunicationException ex) {
                // Expected failure from DNS, don't care
                // But keep a short negative cache to avoid flooding
                Element e = new Element(addr.toString(), null);
                e.setTimeToLive(timeout * 5);
                hostCache.put(e);
            } catch (NamingException e) {
                throw new ProcessorException("unresolvable name " + addr, e);
            } 
        }
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the url
     */
    public String getResolver() {
        return url;
    }

    /**
     * @param url the url to set
     */
    public void setResolver(String resolver) {
        this.url = "dns://" + resolver;
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
     * @return the ttl
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * @param ttl the ttl to set
     */
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

}
