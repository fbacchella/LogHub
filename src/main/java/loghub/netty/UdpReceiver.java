package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;
import loghub.Event;
import loghub.Pipeline;

public abstract class UdpReceiver<F> extends NettyIpReceiver<UdpServer<F>, UdpFactory, Bootstrap, Channel, DatagramChannel, DatagramChannel, F> {

    private static Map<SocketAddress, UdpServer<?>> servers = new HashMap<>();

    public UdpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected UdpServer<F> getServer() {
        SocketAddress addr = getAddress();
        UdpServer<F> server = null;
        if (servers.containsKey(addr)) {
            server = (UdpServer<F>) servers.get(addr);
        } else {
            server = new UdpServer<F>();
            server.setIpAddr((InetSocketAddress) addr);
            servers.put(addr, server);
        }
        return server;
    }

}
