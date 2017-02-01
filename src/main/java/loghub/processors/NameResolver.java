package loghub.processors;

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

public class NameResolver extends AbstractNameResolver {

    private ThreadLocal<DirContext> localContext = new ThreadLocal<DirContext>(){
        @Override
        protected DirContext initialValue() {
            Hashtable<String, String> env = new Hashtable<>();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            env.put("java.naming.provider.url", url);
            env.put("com.sun.jndi.dns.timeout.initial", Integer.toString(timeout * 1000));
            env.put("com.sun.jndi.dns.timeout.retries", Integer.toString(retries));
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
        CacheConfiguration config = properties.getDefaultCacheConfig("NameResolver", this)
                .eternal(false)
                .maxEntriesLocalHeap(cacheSize)
                .timeToLiveSeconds(ttl)
                ;

        hostCache = properties.getCache(config);
        return super.configure(properties);
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the dns pseudo-URL of the resolvers
     */
    public String getResolversUrl() {
        return url;
    }

    /**
     * @param url the dns pseudo-URL of the resolvers
     */
    public void setResolversUrl(String url) {
        this.url = url;
    }

    /**
     * @return the the IP of the resolver
     */
    public String getResolver() {
        return url.replaceFirst(" .*", "").replace("dns://", "");
    }

    /**
     * @param resolver The IP of the resolver
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

    /**
     * @return the timeout for queries in seconds
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout The timeout for queries in seconds
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean resolve(Event event, String query, String destination) throws ProcessorException {
        Element cacheElement = hostCache.get(query);

        if(cacheElement != null && ! cacheElement.isExpired()) {
            Object o = cacheElement.getObjectValue();
            //Set only if it was not a negative cache
            if(o != null) {
                event.put(destination, o);
            }
            return true;
        }
        try {
            Attributes attrs = localContext.get().getAttributes(query, new String[] { type });
            for (NamingEnumeration<? extends Attribute> ae = attrs.getAll(); ae.hasMoreElements();) {
                Attribute attr = (Attribute) ae.next();
                if (attr.getID() != type) {
                    continue;
                }
                Object o = attr.getAll().next();
                if (o != null) {
                    String value = attr.getAll().next().toString();
                    value = value.substring(0, value.length() - 1).intern();
                    hostCache.put(new Element(query, value));
                    event.put(destination, value);
                }
            }
            return true;
        } catch (IllegalArgumentException e) {
            throw event.buildException("can't setup resolver for '" + query + "':" + e.getCause().getMessage());
        } catch (NameNotFoundException ex) {
            // Expected failure from DNS, don't care
            // But keep a short negative cache to avoid flooding
            Element e = new Element(query, null);
            e.setTimeToLive(timeout * 5);
            hostCache.put(e);
            return false;
        } catch (NamingException e) {
            throw event.buildException("unresolvable name '" + query + "': " + e.getMessage());
        } 
    }


}
