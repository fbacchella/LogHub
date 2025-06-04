package loghub.netty.transport;

import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.sctp.nio.NioSctpChannel;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NioPollerServiceProvider implements PollerServiceProvider {

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalServerChannel();
        case TCP: return new NioServerSocketChannel();
        case SCTP: return new NioSctpServerChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }
    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case TCP: return new NioSocketChannel();
        case UDP: return new NioDatagramChannel();
        case SCTP: return new NioSctpChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public Supplier<IoHandlerFactory> getFactorySupplier() {
        return NioIoHandler::newFactory;
    }

    @Override
    public POLLER getPoller() {
        return POLLER.NIO;
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
