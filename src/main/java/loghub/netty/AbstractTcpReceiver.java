package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractTcpServer;

public abstract class AbstractTcpReceiver<R extends AbstractTcpReceiver<R, S, B>,
                                          S extends AbstractTcpServer<S, B>,
                                          B extends AbstractTcpServer.Builder<S, B>
                                         > extends NettyIpReceiver<R, S, B, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, ByteBuf> {

    private int backlog = 16;

    public AbstractTcpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    public AbstractTcpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline, S server) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties, B builder) {
        builder.setBacklog(backlog);
        return super.configure(properties, builder);
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

    /**
     * @return the backlog
     */
     public int getListenBacklog() {
        return backlog;
    }

    /**
     * @param backlog the backlog to set
     */
     public void setListenBacklog(int backlog) {
         this.backlog = backlog;
     }

}
