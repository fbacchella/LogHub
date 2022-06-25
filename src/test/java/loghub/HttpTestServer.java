package loghub;

import java.util.Arrays;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import io.netty.channel.ChannelPipeline;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.HttpHandler;

public class HttpTestServer extends ExternalResource {

    private static Logger logger = LogManager.getLogger();

    private AbstractHttpServer server;
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

    private static class CustomServer extends AbstractHttpServer {
        private static class Builder extends AbstractHttpServer.Builder<CustomServer, CustomServer.Builder> {
            HttpHandler[] handlers = null;
            @Override
            public CustomServer build() throws IllegalArgumentException, InterruptedException {
                return new CustomServer(this);
            }
        }
        private final HttpHandler[] handlers;
        protected CustomServer(CustomServer.Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
            this.handlers = builder.handlers;
        }
        @Override
        public void addModelHandlers(ChannelPipeline p) {
            Arrays.stream(handlers).forEach( i-> p.addLast(i));
        }
    }

    @Override
    protected void before() throws Throwable {
        CustomServer.Builder builder = new CustomServer.Builder();
        builder.handlers = handlers;
        builder.setThreadPrefix("TestServer");
        server = builder.setPort(port).setSSLContext(ssl).withSSL(ssl != null).build();
    }

    @Override
    protected void after() {
        server.close();
    }

}
