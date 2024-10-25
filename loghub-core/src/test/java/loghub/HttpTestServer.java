package loghub;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import io.netty.channel.ChannelPipeline;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.HttpHandler;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TcpTransport;

public class HttpTestServer extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    private TcpTransport transport = null;
    private HttpHandler[] modelHandlers = new HttpHandler[] {};

    public final void setModelHandlers(HttpHandler... handlers) {
        this.modelHandlers = Arrays.copyOf(handlers, handlers.length);
    }

    public URL startServer(TcpTransport.Builder config) {
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                               .setModelSetup(this::addModelHandlers)
                                               .setLogger(logger)
                                               .build();
        config.setConsumer(consumer);
        config.setEndpoint("localhost");
        config.setPort(Tools.tryGetPort());
        transport = config.build();
        try {
            transport.bind();
            String listen = transport.getEndpoint();
            boolean ssl = transport.isWithSsl();
            return new URI(String.format("%s://%s:%d/", ssl ? "https" : "http", listen, transport.getPort())).toURL();
        } catch (URISyntaxException | MalformedURLException ex) {
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
        Optional.ofNullable(transport).ifPresent(NettyTransport::close);
    }

}
