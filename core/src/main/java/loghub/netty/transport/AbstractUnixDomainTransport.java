package loghub.netty.transport;

import io.netty.channel.unix.DomainSocketAddress;

public abstract class AbstractUnixDomainTransport<M, T extends AbstractUnixDomainTransport<M, T, B>, B extends AbstractUnixDomainTransport.Builder<M, T, B>> extends NettyTransport<DomainSocketAddress, M, T, B> {

    protected AbstractUnixDomainTransport(B builder) {
        super(builder);
    }

    public abstract static class Builder<M, T extends AbstractUnixDomainTransport<M,T, B>, B extends AbstractUnixDomainTransport.Builder<M, T, B>> extends NettyTransport.Builder<DomainSocketAddress, M, T, B> {
    }

    protected DomainSocketAddress resolveAddress() {
        return new DomainSocketAddress(endpoint);
    }

}
