package loghub.netty;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.xbill.DNS.Message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;

public class DnsReceiver extends UdpReceiver<Message>{

    public DnsReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    protected ChannelInboundHandlerAdapter getNettyDecoder() {
        return new MessageToMessageDecoder<DatagramPacket>() {

            @Override
            protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
                ByteBuf content = msg.content();
                Message m;
                if(content.isDirect()) {
                    int length = content.readableBytes();
                    byte[] buffer = new byte[length];
                    content.getBytes(content.readerIndex(), buffer);
                    m = new Message(buffer);
                } else {
                    m = new Message(msg.content().array());
                }
                out.add(m);
            }

        };
    }

    protected void populate(Event event, ChannelHandlerContext ctx, Message msg) {
        event.put("message", msg);
    }

    @Override
    public String getReceiverName() {
        return "DNSReceiver/" + getAddress();
    }

}
