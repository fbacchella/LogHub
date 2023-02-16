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
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
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
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.CacheManager;
import loghub.events.Event;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@BuilderClass(NettyNameResolver.Builder.class)
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

        DnsCacheEntry(AddressedEnvelope<DnsResponse, InetSocketAddress> envelope) {
            DnsResponse resp = envelope.content();
            // Some peoples return CNAME in PTR request so check we got the requested PTR
            DnsRecord tmpAnswserRr = null;
            int respCount = resp.count(DnsSection.ANSWER);
            for (int i = 0; i < respCount; i++) {
                tmpAnswserRr = resp.recordAt(DnsSection.ANSWER, i);
                if (tmpAnswserRr.type() == DnsRecordType.PTR) {
                    break;
                } else {
                    // Ensure that tmpAnswserRr will contain null if no PTR returned
                    tmpAnswserRr = null;
                }
            }
            answserRr = tmpAnswserRr;
            questionRr = resp.recordAt((DnsSection.QUESTION));
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

    public static class Builder extends AsyncFieldsProcessor.Builder<NettyNameResolver, AddressedEnvelope<DnsResponse,InetSocketAddress>, Future<AddressedEnvelope<DnsResponse,InetSocketAddress>>> {
        private boolean defaultResolver = true;
        private String etcResolvConf = null;
        private String etcResolverDir = null;
        private String resolver = null;
        @Setter
        private int cacheSize = 10000;
        @Setter
        private CacheManager cacheManager = new CacheManager(getClass().getClassLoader());

        @Override
        public NettyNameResolver build() {
            return new NettyNameResolver(this);
        }

        public void setDefaultResolver(boolean defaultResolver) {
            this.defaultResolver = defaultResolver;
            if (defaultResolver) {
                etcResolvConf = null;
                etcResolverDir = null;
                resolver = null;
            }
        }

        public void setEtcResolvConf(String etcResolvConf) {
            this.etcResolvConf = etcResolvConf;
            this.defaultResolver = false;
            this.resolver = null;
        }

        public void setEtcResolverDir(String etcResolverDir) {
            this.etcResolverDir = etcResolverDir;
            this.defaultResolver = false;
            this.resolver = null;
        }

        public void setResolver(String resolver) {
            this.resolver = resolver;
            etcResolvConf = null;
            etcResolverDir = null;
            this.defaultResolver = false;
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final DnsNameResolver dnsResolver;
    private final Cache<DnsCacheKey, DnsCacheEntry> hostCache;

    public NettyNameResolver(Builder builder) {
        super(builder);
        DnsNameResolverBuilder resolverBuilder = new DnsNameResolverBuilder(EVENTLOOPGROUP.next())
                                                 .queryTimeoutMillis(getTimeout() * 1000L)
                                                 .channelFactory(() -> (DatagramChannel) POLLER.DEFAULTPOLLER.clientChannelProvider(TRANSPORT.UDP))
                                                 .socketChannelFactory(() -> (SocketChannel) POLLER.DEFAULTPOLLER.clientChannelProvider(TRANSPORT.TCP))
                ;
        InetSocketAddress resolverAddr;
        Object parent;
        if (builder.etcResolvConf != null && builder.etcResolverDir == null) {
            try {
                resolverBuilder = resolverBuilder.nameServerProvider(new UnixResolverDnsServerAddressStreamProvider(Paths.get(builder.etcResolvConf).toFile()));
                parent = builder.etcResolvConf;
            } catch (IOException ex) {
                throw new IllegalArgumentException(String.format("Unusable resolv.conf '%s'", builder.etcResolvConf));
            }
        } else if (builder.etcResolvConf != null && builder.etcResolverDir != null) {
            try {
                resolverBuilder = resolverBuilder.nameServerProvider(new UnixResolverDnsServerAddressStreamProvider(builder.etcResolvConf, builder.etcResolverDir));
                parent = builder.etcResolvConf + "/" + builder.etcResolverDir;
            } catch (IOException ex) {
                throw new IllegalArgumentException(String.format("Unusable resolv.conf/resolvers dir' %s/%s'", builder.etcResolvConf, builder.etcResolverDir));
            }
        } else if (builder.resolver != null) {
            try {
                resolverAddr = new InetSocketAddress(InetAddress.getByName(builder.resolver), 53);
                resolverBuilder = resolverBuilder.nameServerProvider(new SingletonDnsServerAddressStreamProvider(resolverAddr));
                parent = resolverAddr;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("Unknown resolver '%s'", builder.resolver));
            }
        } else if (builder.defaultResolver) {
            resolverBuilder = resolverBuilder.nameServerProvider(DnsServerAddressStreamProviders.platformDefault());
            parent = "platformDefault";
        } else {
            throw new IllegalArgumentException("No resolver enabled");
        }
        dnsResolver = resolverBuilder.build();
        hostCache = builder.cacheManager.getBuilder(DnsCacheKey.class, DnsCacheEntry.class)
                           .setName("NameResolver", parent)
                           .setCacheSize(builder.cacheSize)
                           .build();
    }

    @Override
    public Object fieldFunction(Event event, Object addr) throws ProcessorException {
        InetAddress ipaddr = null;
        String toresolv = null;

        // If a string was given, convert it to an Inet?Address
        if (addr instanceof String) {
            try {
                ipaddr = Helpers.parseIpAddress((String) addr);
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
            for (int i = parts.length - 1; i >= 0; i--) {
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
            // if addr was a String, it's used as a hostname
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
    public Object asyncProcess(Event ev, AddressedEnvelope<DnsResponse, InetSocketAddress> envelope) {
        try {
            DnsResponse response = envelope.content();
            DnsQuestion questionRr = response.recordAt((DnsSection.QUESTION));
            return hostCache.invoke(new DnsCacheKey(questionRr), this::addEntry, envelope);
        } finally {
            envelope.release();
        }
    }

    private Object addEntry(MutableEntry<NettyNameResolver.DnsCacheKey,NettyNameResolver.DnsCacheEntry> me, Object[] args) {
        if (! me.exists()) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> envelope = (AddressedEnvelope<DnsResponse, InetSocketAddress>) args[0];
            me.setValue(new DnsCacheEntry(envelope));
        }
        return store(me.getValue());
    }

    private Object store(DnsCacheEntry value) {
        if (value.code == DnsResponseCode.NOERROR && value.answserRr instanceof DnsPtrRecord) {
            // DNS responses end the query with a ., substring removes it.
            DnsPtrRecord ptr = (DnsPtrRecord) value.answserRr;
            return ptr.hostname().substring(0, ptr.hostname().length() - 1);
        } else {
            logger.debug("Failed query for {}: {}", value.questionRr::name, () -> value.code);
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
     *
     * @param query
     * @return
     * @throws Throwable
     */
    DnsRecord warmUp(String query) throws Throwable {
        AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe = null;
        try {
            DnsQuestion dnsquery = new DefaultDnsQuestion(query, DnsRecordType.PTR);
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
