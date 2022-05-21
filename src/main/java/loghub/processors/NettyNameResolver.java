package loghub.processors;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import javax.cache.Cache;
import javax.cache.processor.MutableEntry;

import io.netty.channel.AddressedEnvelope;
import io.netty.channel.EventLoopGroup;
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
import io.netty.resolver.dns.DnsServerAddressStreamProviders;
import io.netty.resolver.dns.SingletonDnsServerAddressStreamProvider;
import io.netty.resolver.dns.UnixResolverDnsServerAddressStreamProvider;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.netty.POLLER;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

public class NettyNameResolver extends AsyncFieldsProcessor<AddressedEnvelope<DnsResponse,InetSocketAddress>, Future<AddressedEnvelope<DnsResponse,InetSocketAddress>>> {

    @EqualsAndHashCode
    private static class DnsCacheKey {
        private final String query;
        private final DnsRecordType type;
        public DnsCacheKey(DnsQuestion query) {
            this.query = query.name();
            this.type = query.type();
        }
        @Override
        public String toString() {
            return "[" + query + " IN " + type.name() + "]";
        }
    }

    private static class DnsCacheEntry {
        final DnsRecord answserRr;
        final DnsQuestion questionRr;
        final DnsResponseCode code;
        final Instant eol;

        DnsCacheEntry(AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe) {
            DnsResponse resp = enveloppe.content();
            // Some peoples return CNAME in PTR request so check we got the requested PTR
            DnsRecord tmpAnswserRr = null;
            int respCount = resp.count(DnsSection.ANSWER);
            for (int i = 0; i < respCount; i++) {
                tmpAnswserRr = resp.recordAt(DnsSection.ANSWER, i);
                if (tmpAnswserRr.type() == DnsRecordType.PTR) {
                    break;
                } else {
                    // Ensure that tmpAnswserRr will contains null if no PTR returned
                    tmpAnswserRr = null;
                }
            }
            answserRr = tmpAnswserRr;
            questionRr = (DnsQuestion) resp.recordAt((DnsSection.QUESTION));
            code = resp.code();
            // Default to 5s on failure, just to avoid wild loop
            // Also check than the answerRR is not null, some servers are happy to return ok on failure
            int ttl = (code.intValue() == NOERROR && answserRr != null) ? (int) answserRr.timeToLive() : 5;
            eol = Instant.now().plus(ttl, ChronoUnit.SECONDS);
            assert ! (answserRr instanceof ReferenceCounted);
            assert ! (questionRr instanceof ReferenceCounted);
            assert ! (code instanceof ReferenceCounted);
        }

        @Override
        public String toString() {
            return "DnsCacheEntry [questionRr=" + questionRr + ", answserRr=" + answserRr + ", code=" + code + "]";
        }
    }

    private static final int NOERROR = DnsResponseCode.NOERROR.intValue();
    private static final EventLoopGroup EVENTLOOPGROUP = POLLER.DEFAULTPOLLER.getEventLoopGroup(1, ThreadBuilder.get().setDaemon(false).getFactory("dnsresolver"));
    private static final VarFormatter reverseFormatV4 = new VarFormatter("${#1%d}.${#2%d}.${#3%d}.${#4%d}.in-addr.arpa.");
    private static final VarFormatter reverseFormatV6 = new VarFormatter("${#1%x}.${#2%x}.");

    @Getter @Setter
    private boolean defaultResolver = true;
    @Getter @Setter
    private String etcResolvConf = null;
    @Getter @Setter
    private String etcResolverDir = null;
    @Getter @Setter
    private String resolver = null;
    @Getter @Setter
    private int cacheSize = 10000;
    @Getter @Setter
    private String poller = null;

    private DnsNameResolver dnsResolver;
    private Cache<DnsCacheKey, DnsCacheEntry> hostCache;

    @Override
    public boolean configure(Properties properties) {
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder(EVENTLOOPGROUP.next())
                        .queryTimeoutMillis(getTimeout() * 1000L)
                        .channelFactory(POLLER.DEFAULTPOLLER::datagramChannelProvider)
                        .socketChannelFactory(POLLER.DEFAULTPOLLER::clientChannelProvider)
                        ;
        InetSocketAddress resolverAddr = null;
        Object parent = null;
        if (etcResolvConf != null && etcResolverDir == null) {
            try {
                builder = builder.nameServerProvider(new UnixResolverDnsServerAddressStreamProvider(Paths.get(etcResolvConf).toFile()));
                parent = etcResolvConf;
            } catch (IOException ex) {
                logger.error("Unusable resolv.conf {}: {}", etcResolvConf, Helpers.resolveThrowableException(ex));
                return false;
            }
        } else if (etcResolvConf != null && etcResolverDir != null) {
            try {
                builder = builder.nameServerProvider(new UnixResolverDnsServerAddressStreamProvider(etcResolvConf, etcResolverDir));
                parent = etcResolvConf + "/" + etcResolverDir;
            } catch (IOException ex) {
                logger.error("Unusable resolv.conf/resolvers dir {}/{}: {}", etcResolvConf, etcResolverDir, Helpers.resolveThrowableException(ex));
                return false;
            }
        } else if (resolver != null) {
            try {
                resolverAddr = new InetSocketAddress(InetAddress.getByName(getResolver()), 53);
                builder = builder.nameServerProvider(new SingletonDnsServerAddressStreamProvider(resolverAddr));
                parent = resolverAddr;
            } catch (UnknownHostException e) {
                logger.error("Unknown resolver '{}': {}", getResolver(), e.getMessage());
                return false;
            }
        } else if (defaultResolver) {
            builder = builder.nameServerProvider(DnsServerAddressStreamProviders.platformDefault());
            parent = "platformDefault";
        } else {
            logger.error("No resolver enabled");
            return false;
        }
        dnsResolver = builder.build();
        hostCache = properties.cacheManager.getBuilder(DnsCacheKey.class, DnsCacheEntry.class)
                        .setName("NameResolver", parent)
                        .setCacheSize(cacheSize)
                        .build();

        return super.configure(properties);
    }

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        InetAddress ipaddr = null;
        String toresolv = null;

        // If a string was given, convert it to a Inet?Address
        if (addr instanceof String) {
            try {
                ipaddr = Helpers.parseIpAddres((String) addr);
            } catch (UnknownHostException e) {
                throw event.buildException("invalid IP address '" + addr + "': " + e.getMessage(), e);
            }
        } else if (addr instanceof InetAddress) {
            ipaddr = (InetAddress) addr;
        }
        if (ipaddr instanceof Inet4Address) {
            Inet4Address ipv4 = (Inet4Address) ipaddr;
            byte[] parts = ipv4.getAddress();
            // the & 0xFF is needed because bytes are signed bytes
            toresolv = reverseFormatV4.format(Arrays.asList(parts[3] & 0xFF , parts[2] & 0xFF , parts[1] & 0xFF, parts[0] & 0xFF));
        } else if (ipaddr instanceof Inet6Address) {
            Inet6Address ipv6 = (Inet6Address) ipaddr;
            byte[] parts = ipv6.getAddress();
            StringBuilder buffer = new StringBuilder();
            for(int i = parts.length - 1; i >= 0; i--) {
                buffer.append(reverseFormatV6.format(Arrays.asList(parts[i] & 0x0F, (parts[i] & 0xF0) >> 4)));
            }
            buffer.append("ip6.arpa");
            toresolv = buffer.toString();
        }

        if (toresolv != null) {
            //If a query was build, use it
            DnsQuestion dnsquery = new DefaultDnsQuestion(toresolv, DnsRecordType.PTR);
            Object found = hostCache.invoke(new DnsCacheKey(dnsquery), this::checkTTL);
            if (found != null) {
                return found;
            } else {
                Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> future = dnsResolver.query(dnsquery);
                throw new AsyncProcessor.PausedEventException(future);
            }
        } else if (addr instanceof String) {
            // if addr was a String, it's used as an hostname
            return addr;
        } else {
            return false;
        }
    }

    private Object checkTTL(MutableEntry<NettyNameResolver.DnsCacheKey,NettyNameResolver.DnsCacheEntry> i, Object[] j) {
        if (i.exists() && i.getValue().eol.isBefore(Instant.now())) {
            i.remove();
            return null;
        } else if (i.exists()) {
            return store(i.getValue());
        } else {
            return null;
        }
    }

    @Override
    public Object asyncProcess(Event ev, AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe) throws ProcessorException {
        try {
            DnsResponse response = enveloppe.content();
            DnsQuestion questionRr = (DnsQuestion) response.recordAt((DnsSection.QUESTION));
            return hostCache.invoke(new DnsCacheKey(questionRr), this::addEntry, enveloppe);
        } finally {
            enveloppe.release();
        }
    }

    private Object addEntry(MutableEntry<NettyNameResolver.DnsCacheKey,NettyNameResolver.DnsCacheEntry> me, Object[] args) {
        if (! me.exists()) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe = (AddressedEnvelope<DnsResponse, InetSocketAddress>) args[0];
            me.setValue(new DnsCacheEntry(enveloppe));
        }
        return store(me.getValue());
    }

    private Object store(DnsCacheEntry value) {
        if (value.answserRr instanceof DnsPtrRecord) {
            // DNS responses end the query with a ., substring removes it.
            DnsPtrRecord ptr = (DnsPtrRecord) value.answserRr;
            return ptr.hostname().substring(0, ptr.hostname().length() - 1);
        } else {
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
    }

    @Override
    public boolean manageException(Event event, Exception ex, VariablePath destination) throws ProcessorException {
        if (ex instanceof DnsNameResolverException) {
            return false;
        } else {
            throw event.buildException("name resolution failed: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    /**
     * Used by test to warm up the cache
     * @param query
     * @param type
     * @return
     * @throws Throwable
     */
    DnsRecord warmUp(String query, DnsRecordType type) throws Throwable {
        AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe = null;
        try {
            DnsQuestion dnsquery = new DefaultDnsQuestion(query, type);
            Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> future = dnsResolver.query(dnsquery);
            enveloppe = future.get();
            hostCache.put(new DnsCacheKey(dnsquery), new DnsCacheEntry(enveloppe));
            return enveloppe.content().recordAt((DnsSection.ANSWER));
        } catch (ExecutionException e) {
            throw e.getCause();
        } finally {
            if (enveloppe != null) {
                enveloppe.release();
            }
        }
    }

    @Override
    public BiConsumer<Event, Future<AddressedEnvelope<DnsResponse, InetSocketAddress>>> getTimeoutHandler() {
        // Self-timeout handler, no external help needed
        return null;
    }

}
