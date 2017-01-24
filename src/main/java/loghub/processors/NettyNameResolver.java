package loghub.processors;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.AddressedEnvelope;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DnsPtrRecord;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponse;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.HostsFileEntriesResolver;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverException;
import io.netty.resolver.dns.DnsServerAddresses;
import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class NettyNameResolver extends AbstractNameResolver implements AsyncProcessor<AddressedEnvelope<DnsResponse,InetSocketAddress>> {

    private static final EventLoopGroup evg = new NioEventLoopGroup();
    private int timeout = 1;
    private DnsServerAddresses resolverAddress = DnsServerAddresses.defaultAddresses();
    private DnsNameResolver resolver;
    private final Map<Event, String> destinations = new ConcurrentHashMap<>();

    @Override
    public boolean configure(Properties properties) {
        if (resolverAddress == null) {
            try {
                resolverAddress = DnsServerAddresses.rotational(new InetSocketAddress(InetAddress.getByName(getResolver()), 53));
            } catch (UnknownHostException e) {
                logger.error("Unknown resolver '{}': {}", getResolver(), e.getMessage());
                return false;
            }
        }

        resolver = new DnsNameResolver(
                evg.next(),
                new ChannelFactory<DatagramChannel>() {
                    @Override 
                    public DatagramChannel newChannel() {
                        return new NioDatagramChannel();
                    }
                },
                resolverAddress,
                new DefaultDnsCache(),
                timeout * 1000,
                new InternetProtocolFamily[] { InternetProtocolFamily.IPv4 },
                true,
                10,
                false,
                65535,
                false,
                HostsFileEntriesResolver.DEFAULT,
                new String[]{},
                0);
        return super.configure(properties);
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
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
        String destination = destinations.remove(ev);
        DnsRecord rr = enveloppe.content().recordAt((DnsSection.ANSWER));
        if (rr !=null && rr instanceof DnsPtrRecord) {
            DnsPtrRecord ptr = (DnsPtrRecord) rr;
            // DNS responses end the query with a ., substring removes it.
            ev.put(destination, ptr.hostname().substring(0, ptr.hostname().length() - 1));
            return true;
        } else {
            return false;
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

    @Override
    public void setResolver(String resolver) {
        resolverAddress = null;
        super.setResolver(resolver);
    }


}
