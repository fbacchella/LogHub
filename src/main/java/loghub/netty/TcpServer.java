package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public abstract class TcpServer<F> extends AbstractIpNettyServer<TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, F> {

    private POLLER poller = POLLER.NIO;

    public TcpServer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected TcpFactory getFactory(Properties properties) {
        return new TcpFactory(poller);
    }

    public String getPoller() {
        return poller.toString();
    }

    public void setPoller(String poller) {
        this.poller = POLLER.valueOf(poller);
    }

}
