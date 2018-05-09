package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.netty.servers.TcpServer;

public abstract class GenericTcp extends NettyIpReceiver<TcpServer, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, ByteBuf> {

    private TcpServer server;
    private int backlog = 16;

    public GenericTcp(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    public GenericTcp(BlockingQueue<Event> outQueue, Pipeline pipeline, TcpServer server) {
        super(outQueue, pipeline);
    }

    protected void setServer(TcpServer server) {
        this.server = server;
    }

    @Override
    protected TcpServer getServer() {
        return server;
    }

    @Override
    public void addOptions(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    protected ByteBuf getContent(ByteBuf message) {
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
