package loghub;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLSession;

public class IpConnectionContext extends ConnectionContext<InetSocketAddress> {

    private final InetSocketAddress remoteaddr;
    private final InetSocketAddress localaddr;
    private final SSLSession session;

    public IpConnectionContext(InetSocketAddress localaddr, InetSocketAddress remoteaddr, SSLSession session) {
        this.localaddr = localaddr;
        this.remoteaddr = remoteaddr;
        this.session = session;
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

}
