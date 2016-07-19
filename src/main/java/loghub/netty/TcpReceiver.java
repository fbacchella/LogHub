package loghub.netty;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public abstract class TcpReceiver<F> extends NettyIpReceiver<TcpServer<F>, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, F> {

    private ByteToMessageDecoder nettydecoder; 

    public TcpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        if (nettydecoder == null && decoder != null) {
            nettydecoder = new ByteToMessageDecoder() {
                @Override
                protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                    Map<String, Object> content = decoder.decode(in);
                    out.add(content);
                }
            };
        }
        return super.configure(properties);
    }

}
