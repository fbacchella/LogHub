package loghub;

import java.util.Arrays;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import io.netty.channel.ChannelPipeline;
import loghub.netty.http.HttpHandler;
import loghub.netty.servers.HttpServer;
import lombok.Setter;
import lombok.SneakyThrows;

public class HttpTestServer extends ExternalResource {

    private static Logger logger = LogManager.getLogger();

    private HttpServer server;
    private final HttpHandler[] handlers;
    private SSLContext ssl;
    private int port;

    @SafeVarargs
    public HttpTestServer(SSLContext ssl, int port, HttpHandler... handlers) {
        logger.debug("Starting a test HTTP servers on port {}, protocol {}", () -> port, () -> ssl != null ? "https" : "http");
        this.handlers = Arrays.copyOf(handlers, handlers.length);
        this.ssl = ssl;
        this.port = port;
    }

    public static class CustomServer extends HttpServer {
        public static class Builder extends HttpServer.Builder<CustomServer> {
            @Setter
            HttpHandler[] handlers = new HttpHandler[]{};
            @Setter
            String threadPrefix = "JunitTests";
            @Override
            public CustomServer build()  {
                return new CustomServer(this);
            }
        }
        @Setter
        private final HttpHandler[] handlers;
        protected CustomServer(CustomServer.Builder builder) {
            super(builder);
            config.setThreadPrefix("TestServer");
            this.handlers = builder.handlers;
        }
        @Override
        public void addModelHandlers(ChannelPipeline p) {
            Arrays.stream(handlers).forEach(p::addLast);
        }
    }

    @Override
    protected void before() throws Throwable {
        CustomServer.Builder builder = new CustomServer.Builder();
        builder.handlers = handlers;
        server = builder.setPort(port).setSslContext(ssl).setWithSSL(ssl != null).build();
        server.start();
    }

    @SneakyThrows
    @Override
    protected void after() {
        server.stop();
    }

}
