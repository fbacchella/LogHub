package loghub;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import loghub.cloners.DeepCloner;

public class IpConnectionContext extends BuildableConnectionContext<InetSocketAddress> {

    static {
        DeepCloner.registerImmutable(IpConnectionContext.class);
    }

    private final InetSocketAddress remoteaddr;
    private final InetSocketAddress localaddr;
    private final SSLSession session;

    public IpConnectionContext(InetSocketAddress localaddr, InetSocketAddress remoteaddr, SSLSession session) {
        this.localaddr = localaddr;
        this.remoteaddr = remoteaddr;
        this.session = session;
    }

    public IpConnectionContext(InetSocketAddress localaddr, InetSocketAddress remoteaddr) {
        this.localaddr = localaddr;
        this.remoteaddr = remoteaddr;
        this.session = null;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localaddr;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteaddr;
    }

    public SSLSession getSslParameters() {
        return session;
    }

    @Override
    public Map<String, Object> getProperties() {
        return session != null ? Map.of("sslParameters", session) : Map.of();
    }

    @Override
    public <T> Optional<T> getProperty(String property) {
        return "sslParameters".equals(property) ? (Optional<T>) Optional.ofNullable(session) : super.getProperty(property);
    }

}
