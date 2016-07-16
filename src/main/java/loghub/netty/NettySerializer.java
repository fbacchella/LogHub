package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public class NettySerializer extends TcpServer<Object> {

    private ClassResolver resolver;

    public NettySerializer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        resolver = ClassResolvers.softCachingConcurrentResolver(properties.classloader);
        return super.configure(properties);
    }

    @Override
    protected ByteToMessageDecoder getNettyDecoder() {
        return new ObjectDecoder(resolver);
    }

    @Override
    protected void populate(Event event, ChannelHandlerContext ctx, Object msg) {
        event.put("message", msg);
        event.put("class", msg.getClass());
        SocketAddress addr = ctx.channel().remoteAddress();
        if(addr instanceof InetSocketAddress) {
            event.put("host", ((InetSocketAddress) addr).getAddress());
        }
    }

}
