package loghub.netty.transport;

import java.util.Objects;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalIoHandler;
import io.netty.channel.local.LocalServerChannel;

public class LocalPollerServiceProvider implements PollerServiceProvider {

    @Override
    public POLLER getPoller() {
        return POLLER.LOCAL;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean isUnixSocket() {
        return false;
    }

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        if (Objects.requireNonNull(transport) == TRANSPORT.LOCAL) {
            return new LocalServerChannel();
        } else {
            throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        if (Objects.requireNonNull(transport) == TRANSPORT.LOCAL) {
            return new LocalChannel();
        } else {
            throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public Supplier<IoHandlerFactory> getFactorySupplier() {
        return LocalIoHandler::newFactory;
    }

}
