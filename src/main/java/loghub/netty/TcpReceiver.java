package loghub.netty;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.netty.servers.TcpServer;

public class TcpReceiver extends NettyIpReceiver<TcpServer, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, ByteBuf> {

    protected ByteToMessageDecoder splitter;
    private final TcpServer server = new TcpServer();
    private int backlog = 16;

    public TcpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected TcpServer getServer() {
        return server;
    }

    @Override
    public void addHandlers(ChannelPipeline pipe) {
        pipe.addFirst("Splitter", splitter);
        super.addHandlers(pipe);
    }

    @Override
    protected void populate(Event event, ChannelHandlerContext ctx, Map<String, Object> msg) {
        event.putAll(msg);
    }


    @Override
    public String getReceiverName() {
        return "TcpNettyReceiver";
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
