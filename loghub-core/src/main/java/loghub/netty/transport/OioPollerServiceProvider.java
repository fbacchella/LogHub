package loghub.netty.transport;

import io.netty.channel.Channel;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ServerChannel;

public class OioPollerServiceProvider implements PollerServiceProvider {

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        throw new UnsupportedOperationException("Deprecated OIO");
    }

    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        throw new UnsupportedOperationException("Deprecated OIO");
    }

    @Override
    public IoHandlerFactory getIoHandlerFactory() {
        throw new UnsupportedOperationException("Deprecated OIO");
    }

    @Override
    public POLLER getPoller() {
        return POLLER.OIO;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean isUnixSocket() {
        return false;
    }
}
