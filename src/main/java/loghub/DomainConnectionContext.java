package loghub;

import io.netty.channel.unix.DomainSocketAddress;

public class DomainConnectionContext extends ConnectionContext<DomainSocketAddress> {

    private final DomainSocketAddress remoteaddr;
    private final DomainSocketAddress localaddr;

    public DomainConnectionContext(DomainSocketAddress localaddr, DomainSocketAddress remoteaddr) {
        this.localaddr = localaddr;
        this.remoteaddr = remoteaddr;
    }

    @Override
    public DomainSocketAddress getLocalAddress() {
        return localaddr;
    }

    @Override
    public DomainSocketAddress getRemoteAddress() {
        return remoteaddr;
    }

}
