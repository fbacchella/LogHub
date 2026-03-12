package loghub;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.HttpHandler;
import loghub.netty.http.HttpProtocolVersion;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TcpTransport;
import loghub.security.AuthenticationHandler;
import lombok.Getter;
import lombok.Setter;

public class HttpTestServer extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    @Getter
    private TcpTransport transport = null;
    private HttpHandler[] modelHandlers = new HttpHandler[] {};
    @Setter
    Consumer<Channel> http2handler = null;
    @Setter
    private BiConsumer<HttpProtocolVersion, ChannelPipeline> versionedModelSetup;
    @Setter
    private AuthenticationHandler authHandler;
   @Getter
    private final Supplier<String> holder = () -> "HttpTestServer";
    @Getter
    private URI uri;

    public final void setModelHandlers(HttpHandler... handlers) {
        this.modelHandlers = Arrays.copyOf(handlers, handlers.length);
    }

    public URI startServer(TcpTransport.Builder config) {
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                               .setAuthHandler(authHandler)
                                               .setVersionedModelSetup(versionedModelSetup)
                                               .setModelSetup(this::addModelHandlers)
                                               .setHttp2handler(http2handler)
                                               .setLogger(logger)
                                               .setHolder(holder)
                                               .build();
        config.setConsumer(consumer);
        config.setEndpoint("localhost");
        config.setPort(Tools.tryGetPort());
        config.setAlpnSelector(consumer.getAlpnSelector());
        transport = config.build();
        try {
            transport.bind();
            String listen = transport.getEndpoint();
            boolean ssl = transport.isWithSsl();
            uri = new URI(String.format("%s://%s:%d/", ssl ? "https" : "http", listen, transport.getPort()));
            return uri;
        } catch (URISyntaxException ex) {
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
    public void after() {
        Optional.ofNullable(transport).ifPresent(NettyTransport::close);
    }

}
