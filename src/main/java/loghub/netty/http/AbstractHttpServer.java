package loghub.netty.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;
import loghub.netty.servers.TcpServer;

public abstract class AbstractHttpServer extends TcpServer implements ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> {

    private int port;
    private InetAddress host = null;

    public ChannelFuture configure(Properties properties) {
        return super.configure(properties, this);
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpContentCompressor(9, 15, 8));
        p.addLast(new ChunkedWriteHandler());
        p.addLast(new HttpObjectAggregator(1048576));
        addModelHandlers(p);
    }

    public abstract void addModelHandlers(ChannelPipeline p);

    @Override
    public void addOptions(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public InetSocketAddress getListenAddress() {
        return new InetSocketAddress(host, port);
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the host
     */
    public InetAddress getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(InetAddress host) {
        this.host = host;
    }

    public void setHost(String host) throws UnknownHostException {
        this.host = InetAddress.getByName(host);
    }

}