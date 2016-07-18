package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.Event;
import loghub.Pipeline;

public abstract class TcpReceiver<F> extends NettyIpReceiver<TcpServer<F>, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, F> {

    public TcpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

}
