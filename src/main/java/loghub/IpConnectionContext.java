package loghub;

import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

public class IpConnectionContext extends ConnectionContext<InetSocketAddress> {

    private final InetSocketAddress remoteaddr;
    private final InetSocketAddress localaddr;
    private final SSLSession session;

    public IpConnectionContext(Socket s) {
        localaddr = (InetSocketAddress) s.getLocalSocketAddress();
        remoteaddr = (InetSocketAddress) s.getRemoteSocketAddress();
        if (s instanceof SSLSocket) {
            SSLSocket ssls = (SSLSocket) s;
            session = ssls.getSession();
        } else {
            session = null;
        }
    }

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
