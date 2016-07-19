package loghub.netty.http;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import loghub.configuration.Properties;
import loghub.netty.HandlersSource;
import loghub.netty.TcpServer;

public class HttpServer extends TcpServer<Object> implements HandlersSource<ServerSocketChannel, SocketChannel> {

    @Override
    public void addChildHandlers(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestDecoder());
        p.addLast(new HttpObjectAggregator(1048576));
        p.addLast(new HttpResponseEncoder());
        p.addLast(new ResourceFiles());
        p.addLast(new NotFound());
    }

    @Override
    public void addHandlers(ServerSocketChannel ch) {
    }

    public ChannelFuture configure(Properties properties) {
        return super.configure(properties, this);
    }

}
