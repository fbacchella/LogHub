package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ServerChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public abstract class TcpServer<D> extends NettyServer<ServerFactory, ServerBootstrap,ServerChannel, D> {
    
    private enum POLLER {
        NIO,
        EPOLL,
    }
    
    private POLLER poller = POLLER.NIO;

    public TcpServer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected ServerFactory getFactory(Properties properties) {
        switch (poller) {
        case NIO:
            return new TcpNioFactory();
        default:
            throw new UnsupportedOperationException();
        }
    }

    public String getPoller() {
        return poller.toString();
    }

    public void setPoller(String poller) {
        this.poller = POLLER.valueOf(poller);
    }

}
