package loghub.netty.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;
import loghub.netty.servers.TcpServer;

public abstract class AbstractHttpServer extends TcpServer implements ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> {

    private int port;
    private String host = null;

    @Override
    public boolean configure(Properties properties, ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> consumer) {
        setThreadFactory(new DefaultThreadFactory("builtinhttpserver/" +
                consumer.getListenAddress().getAddress().getHostAddress() + ":" + consumer.getListenAddress().getPort()));
        return super.configure(properties, consumer);
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("HttpServerCodec", new HttpServerCodec());
        p.addLast("HttpContentCompressor", new HttpContentCompressor(9, 15, 8));
        p.addLast("ChunkedWriteHandler", new ChunkedWriteHandler());
        p.addLast("HttpObjectAggregator", new HttpObjectAggregator(1048576));
        try {
            addModelHandlers(p);
        } catch (ChannelPipelineException e) {
            logger.error("Invalid pipeline configuration: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            p.addAfter("HttpObjectAggregator", "BrokenConfigHandler", new HttpRequestProcessing() {

                @Override
                protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
                    throw new HttpRequestFailure(HttpResponseStatus.SERVICE_UNAVAILABLE, "Unable to process request because of invalid configuration");
                }

                @Override
                protected String getContentType(HttpRequest request, HttpResponse response) {
                    return null;
                }
            });
        }
    }
    }

    public abstract void addModelHandlers(ChannelPipeline p);

    @Override
    public void addOptions(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public InetSocketAddress getListenAddress() {
        try {
            return new InetSocketAddress(host != null ? InetAddress.getByName(host) : null , port);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return null;
        }
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
    public String getHost() {
        return host;
    }

    public void setHost(String host) throws UnknownHostException {
        // Ensure host is null if given empty string, to be resolved as "bind *" by InetSocketAddress;
        this.host = host != null && !host.isEmpty() ? host : null;
    }

}
