package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
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
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        throw new UnsupportedOperationException("Deprecated OIO");
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
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
