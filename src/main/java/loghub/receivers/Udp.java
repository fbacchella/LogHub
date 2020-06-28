package loghub.receivers;

import java.net.InetSocketAddress;
import java.util.stream.Stream;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.configuration.Properties;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyIpReceiver;
import loghub.netty.UdpFactory;
import loghub.netty.servers.UdpServer;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Udp.Builder.class)
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

    public static class Builder extends NettyIpReceiver.Builder<Udp> {
        @Setter
        private int bufferSize = -1;
        @Override
        public Udp build() {
            return new Udp(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final  int bufferSize;

    protected Udp(Builder builder) {
        super(builder);
        this.bufferSize = builder.bufferSize;
    }

    @Override
    protected UdpServer.Builder getServerBuilder() {
        return UdpServer.getBuilder();
    }

    @Override
    public final boolean configure(Properties properties, UdpServer.Builder builder) {
        builder.setBufferSize(bufferSize).setThreadPrefix("UdpNettyReceiver");
        return super.configure(properties, builder);
    }

    @Override
    public String getReceiverName() {
        return "UdpNettyReceiver/" + (getHost() == null ? "*" : getHost()) + ":" + getPort();
    }

    @Override
    public ByteBuf getContent(DatagramPacket message) {
        return message.content();
    }

    @Override
    public Stream<Event> nettyMessageDecode(ChannelHandlerContext ctx,
                                            ByteBuf message) {
        return decodeStream(getConnectionContext(ctx), message);
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
