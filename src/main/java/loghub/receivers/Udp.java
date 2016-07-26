package loghub.receivers;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import loghub.Event;
import loghub.Pipeline;
import loghub.netty.NettyIpReceiver;
import loghub.netty.UdpFactory;
import loghub.netty.servers.UdpServer;

public class Udp extends NettyIpReceiver<UdpServer, UdpFactory, Bootstrap, Channel, DatagramChannel, Channel, DatagramPacket> {

    private static final Map<SocketAddress, UdpServer> servers = new HashMap<>();

    public Udp(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected UdpServer getServer() {
        SocketAddress addr = getListenAddress();
        UdpServer server = null;
        if (servers.containsKey(addr)) {
            server = servers.get(addr);
        } else {
            server = new UdpServer();
            servers.put(addr, server);
        }
        return server;
    }

    @Override
    public String getReceiverName() {
        return "UdpNettyReceiver/" + getListenAddress();
    }

    @Override
    protected ByteBuf getContent(DatagramPacket message) {
        return message.content();
    }

    @Override
    protected Object ResolveSourceAddress(ChannelHandlerContext ctx, DatagramPacket message) {
        return message.sender().getAddress();
    }

    @Override
    protected boolean closeonerror() {
        return false;
    }

}
