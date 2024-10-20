package loghub.receivers;

import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.events.Event;
import loghub.netty.BaseChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.NettyReceiver;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TRANSPORT;
import lombok.Setter;

@BuilderClass(Udp.Builder.class)
public class Udp extends NettyReceiver<Udp, DatagramPacket, Udp.Builder> implements ConsumerProvider {

    @Setter
    public static class Builder extends NettyReceiver.Builder<Udp, DatagramPacket, Udp.Builder> {
        public Builder() {
            setTransport(TRANSPORT.UDP);
        }
        private int bufferSize = -1;
        @Override
        public Udp build() {
            return new Udp(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    protected Udp(Builder builder) {
        super(builder);
    }

    @Override
    protected String getThreadPrefix(Udp.Builder builder) {
        return "UdpNettyReceiver";
    }

    @Override
    protected void tweakNettyBuilder(Udp.Builder builder,
            NettyTransport.Builder<?, DatagramPacket, ?, ?> nettyTransportBuilder) {
        super.tweakNettyBuilder(builder, nettyTransportBuilder);
        nettyTransportBuilder.setBufferSize(builder.bufferSize);
    }

    @Override
    public String getReceiverName() {
        return "UdpReceiver/" + Helpers.ListenString(getListen() + "/" + getPort());
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
    public BaseChannelConsumer<Udp, DatagramPacket, Udp.Builder> getConsumer() {
        return new BaseChannelConsumer<>(this);
    }

}
