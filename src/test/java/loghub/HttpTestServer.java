package loghub;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.HttpHandler;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TransportConfig;

public class HttpTestServer extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    private Optional<NettyTransport<InetSocketAddress, ByteBuf>> transport = Optional.empty();
    private HttpHandler[] modelHandlers = new HttpHandler[] {};

    public final void setModelHandlers(HttpHandler... handlers) {
        this.modelHandlers = Arrays.copyOf(handlers, handlers.length);
    }

    public URL startServer(TransportConfig config) {
        transport = Optional.of(
                TRANSPORT.TCP.<NettyTransport<InetSocketAddress, ByteBuf>, InetSocketAddress, ByteBuf>getInstance(POLLER.DEFAULTPOLLER));
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                               .setModelSetup(this::addModelHandlers)
                                               .setLogger(logger)
                                               .build();
        config.setConsumer(consumer);
        config.setEndpoint("localhost");
        config.setPort(Tools.tryGetPort());
        try {
            transport.get().bind(config);
            String listen = config.getEndpoint();
            boolean ssl = config.isWithSsl();
            return new URL(String.format("%s://%s:%d/", ssl ? "https" : "http", listen, config.getPort()));
        } catch (MalformedURLException ex) {
            throw new IllegalStateException(ex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private void addModelHandlers(ChannelPipeline p) {
        Arrays.stream(modelHandlers).forEach(p::addLast);
    }

    @Override
    protected void after() {
        transport.ifPresent(NettyTransport::close);
    }

}
