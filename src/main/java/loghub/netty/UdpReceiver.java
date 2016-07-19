package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public class UdpReceiver extends NettyIpReceiver<UdpServer<Map<String, Object>>, UdpFactory, Bootstrap, Channel, DatagramChannel, DatagramChannel, Map<String, Object>> {

    private MessageToMessageDecoder<DatagramPacket> nettydecoder;
    private static Map<SocketAddress, UdpServer<?>> servers = new HashMap<>();

    public UdpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected UdpServer<Map<String, Object>> getServer() {
        SocketAddress addr = getAddress();
        UdpServer<Map<String, Object>> server = null;
        if (servers.containsKey(addr)) {
            server = (UdpServer<Map<String, Object>>) servers.get(addr);
        } else {
            server = new UdpServer<Map<String, Object>>();
            server.setIpAddr((InetSocketAddress) addr);
            servers.put(addr, server);
        }
        return server;
    }

    @Override
    public boolean configure(Properties properties) {
        if (nettydecoder == null && decoder != null) {
            nettydecoder = new MessageToMessageDecoder<DatagramPacket>() {
                @Override
                protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
                    Map<String, Object> content = decoder.decode(msg.content());
                    out.add(content);
                }
            };
        }
        return super.configure(properties);
    }

    @Override
    protected ChannelInboundHandlerAdapter getNettyDecoder() {
        return nettydecoder;
    }

    @Override
    protected void populate(Event event, ChannelHandlerContext ctx, Map<String, Object> msg) {
        event.putAll(msg);
    }

    @Override
    public String getReceiverName() {
        return "NettyReceiver";
    }

}
