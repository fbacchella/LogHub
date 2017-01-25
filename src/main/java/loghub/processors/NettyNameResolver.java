package loghub.processors;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.DnsNameResolverException;
import io.netty.resolver.dns.DnsServerAddresses;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class NettyNameResolver extends AbstractNameResolver implements AsyncProcessor<AddressedEnvelope<DnsResponse,InetSocketAddress>> {

    private static final EventLoopGroup evg = new NioEventLoopGroup(1, new DefaultThreadFactory("dnsresolver"));
    private int timeout = 10;
    private DnsNameResolver resolver;
    private final Map<Event, String> destinations = new ConcurrentHashMap<>();

    @Override
    public boolean configure(Properties properties) {
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder(evg.next())
                .queryTimeoutMillis(timeout * 1000)
                .resolveCache(new DefaultDnsCache(timeout * 1000, 3600000, timeout * 1000))
                .channelType(NioDatagramChannel.class)
                ;
        try {
            if (getResolver() != null) {
                builder = builder.nameServerAddresses(DnsServerAddresses.rotational(new InetSocketAddress(InetAddress.getByName(getResolver()), 53)));
            }
        } catch (UnknownHostException e) {
            logger.error("Unknown resolver '{}': {}", getResolver(), e.getMessage());
            return false;
        }
        resolver = builder.build();

        return super.configure(properties);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean resolve(Event event, String query, String destination) throws ProcessorException {
        DnsQuestion dnsquery = new DefaultDnsQuestion(query, DnsRecordType.PTR);
        Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> future = resolver.query(dnsquery);
        destinations.put(event, destination);
        throw new ProcessorException.PausedEventException(event, future);
    }

    @Override
    public boolean process(Event ev, AddressedEnvelope<DnsResponse, InetSocketAddress> enveloppe) throws ProcessorException {
        try {
            String destination = destinations.remove(ev);
            DnsRecord rr = enveloppe.content().recordAt((DnsSection.ANSWER));
            if(rr != null && rr instanceof DnsPtrRecord) {
                DnsPtrRecord ptr = (DnsPtrRecord) rr;
                // DNS responses end the query with a ., substring removes it.
                ev.put(destination, ptr.hostname().substring(0, ptr.hostname().length() - 1));
                return true;
            } else {
                return false;
            } 
        } finally {
            enveloppe.release();
        }
    }

    @Override
    public boolean manageException(Event event, Exception ex) throws ProcessorException {
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

}
