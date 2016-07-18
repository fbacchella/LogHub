package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;

public abstract class AbstractIpNettyServer<A extends ComponentFactory<B, C, D, E>, B extends AbstractBootstrap<B,C>,C extends Channel, D extends Channel, E extends Channel, F> extends AbstractNettyServer<A, B, C, D, E, F> {

    private InetSocketAddress addr;
    protected POLLER poller = POLLER.NIO;

    protected AbstractIpNettyServer(HandlersSource<D, E> source) {
        super(source);
    }

    @Override
    protected SocketAddress getAddress() {
        return addr;
    }

    public InetSocketAddress getIpAddr() {
        return addr;
    }

    public void setIpAddr(InetSocketAddress addr) {
        this.addr = addr;
    }

    public String getPoller() {
        return poller.toString();
    }

    public void setPoller(String poller) {
        this.poller = POLLER.valueOf(poller);
    }

}

