package loghub.receivers;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyIpReceiver;
import loghub.netty.UdpFactory;
import loghub.netty.servers.UdpServer;
import loghub.netty.servers.UdpServer.Builder;

public class Udp extends NettyIpReceiver<Udp,
                                         UdpServer,
                                         UdpServer.Builder,
                                         UdpFactory, 
                                         Bootstrap,
                                         Channel,
                                         DatagramChannel,
                                         Channel,
                                         DatagramPacket
                                         > implements ConsumerProvider<Udp, Bootstrap, Channel> {

    private int buffersize = -1;

    public Udp(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected Builder getServerBuilder() {
        return UdpServer.getBuilder();
    }

    @Override
    public final boolean configure(Properties properties, UdpServer.Builder builder) {
        builder.setBufferSize(buffersize);
        return super.configure(properties, builder);
    }

    @Override
    public String getReceiverName() {
        return "UdpNettyReceiver/" + getListenAddress();
    }

    @Override
    public ByteBuf getContent(DatagramPacket message) {
        return message.content();
    }

    /**
     * @return the buffersize
     */
     public int getBufferSize() {
        return buffersize;
    }

    /**
     * @param buffersize the buffersize to set
     */
     public void setBufferSize(int buffersize) {
         this.buffersize = buffersize;
     }

     @Override
     public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, DatagramPacket message) {
         InetSocketAddress remoteaddr = message.sender();
         InetSocketAddress localaddr = message.recipient();
         return new IpConnectionContext(localaddr, remoteaddr, null);
     }

     @Override
     public BaseChannelConsumer<Udp, Bootstrap, Channel, DatagramPacket> getConsumer() {
         return new BaseChannelConsumer<Udp, Bootstrap, Channel, DatagramPacket>(this);
     }

}
