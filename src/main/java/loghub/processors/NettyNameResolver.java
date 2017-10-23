package loghub.processors;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import io.netty.channel.AddressedEnvelope;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DnsPtrRecord;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponse;
import io.netty.handler.codec.dns.DnsResponseCode;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.DnsNameResolverException;
import io.netty.resolver.dns.SingletonDnsServerAddressStreamProvider;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

public class NettyNameResolver extends AbstractNameResolver implements FieldsProcessor.AsyncFieldsProcessor<AddressedEnvelope<DnsResponse,InetSocketAddress>> {

    private static class DnsCacheKey {
        private final String query;
        private final DnsRecordType type;
        public DnsCacheKey(String query, DnsRecordType type) {
            this.query = query;
            this.type = type;
        }
        public DnsCacheKey(DnsQuestion query) {
            this.query = query.name();
            this.type = query.type();
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((query == null) ? 0 : query.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            DnsCacheKey other = (DnsCacheKey) obj;
            if(query == null) {
                if(other.query != null)
                    return false;
            } else if(!query.equals(other.query))
                return false;
            if(type == null) {
                if(other.type != null)
                    return false;
            } else if(!type.equals(other.type))
                return false;
            return true;
        }
        @Override
        public String toString() {
            return "[" + query + " IN " + type.name() + "]";
        }

    }

    private static final EventLoopGroup evg = new NioEventLoopGroup(1, new DefaultThreadFactory("dnsresolver"));
    private int timeout = 10;
    private DnsNameResolver resolver;
    private int cacheSize = 10000;
    private Cache hostCache;

    @Override
    public boolean configure(Properties properties) {
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder(evg.next())
                .queryTimeoutMillis(timeout * 1000)
                .channelType(NioDatagramChannel.class)
                ;
        try {
            if (getResolver() != null) {
                InetSocketAddress resolverAddr = new InetSocketAddress(InetAddress.getByName(getResolver()), 53);
                builder = builder.nameServerProvider(new SingletonDnsServerAddressStreamProvider(resolverAddr));
            }
        } catch (UnknownHostException e) {
            logger.error("Unknown resolver '{}': {}", getResolver(), e.getMessage());
            return false;
        }
        resolver = builder.build();

        CacheConfiguration config = properties.getDefaultCacheConfig("NameResolver", this)
                .eternal(false)
                .maxEntriesLocalHeap(cacheSize)
                ;

        hostCache = properties.getCache(config);

        return super.configure(properties);
    }

    @Override
    public boolean resolve(Event event, String query, String destination) throws ProcessorException {
        DnsQuestion dnsquery = new DefaultDnsQuestion(query, DnsRecordType.PTR);

        Element e = hostCache.get(new DnsCacheKey(dnsquery));
        if (e != null) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe = (AddressedEnvelope<DnsResponse, InetSocketAddress>) e.getObjectValue();
            if (! e.isExpired()) {
                return process(event, enveloppe , destination);
            } else {
                hostCache.remove(enveloppe);
                enveloppe.release();
            }
        }
        Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> future = resolver.query(dnsquery);
        throw new ProcessorException.PausedEventException(event, future);
    }

    @Override
    public boolean process(Event ev, AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe, String destination) throws ProcessorException {
        try {
            DnsRecord rr = enveloppe.content().recordAt((DnsSection.ANSWER));
            if (rr != null) {
                DnsRecord question = enveloppe.content().recordAt((DnsSection.QUESTION));
                // Default to 5s on failure, just to avoild wild loop
                int ttl = enveloppe.content().code().intValue() == DnsResponseCode.NOERROR.intValue() ? (int) rr.timeToLive() : 5;
                Element cacheElement = new Element(new DnsCacheKey(question.name(),question.type()), enveloppe, ttl / 2, ttl);
                hostCache.put(cacheElement);
                enveloppe.retain();
                if (rr instanceof DnsPtrRecord) {
                    // DNS responses end the query with a ., substring removes it.
                    DnsPtrRecord ptr = (DnsPtrRecord) rr;
                    ev.put(destination, ptr.hostname().substring(0, ptr.hostname().length() - 1).intern());
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } finally {
            enveloppe.release();
        }
    }

    @Override
    public boolean manageException(Event event, Exception ex, String destination) throws ProcessorException {
        if (ex instanceof DnsNameResolverException) {
            return false;
        } else {
            throw event.buildException("name resolution failed: " + ex.getMessage(), ex);
        }
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
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
     * Used by test to warm up the cache
     * @param query
     * @param type
     * @return
     * @throws Throwable
     */
    DnsRecord warmUp(String query, DnsRecordType type) throws Throwable {
        try {
            DnsQuestion dnsquery = new DefaultDnsQuestion(query, type);
            Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> future = resolver.query(dnsquery);
            AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe;
            enveloppe = future.get();
            logger.trace("warmup: {} -> {}", () -> dnsquery, () -> enveloppe);
            Element cachedEntry = new Element(new DnsCacheKey(dnsquery), enveloppe);
            hostCache.put(cachedEntry);
            return enveloppe.content().recordAt((DnsSection.ANSWER));
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

}
