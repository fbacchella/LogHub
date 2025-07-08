package loghub.netty.transport;

import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;
import loghub.FastExternalizeObject.Immutable;

@Immutable
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
