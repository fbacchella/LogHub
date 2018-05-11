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

    private class CustomServer extends AbstractHttpServer {

        protected CustomServer(Builder<CustomServer> builder) {
            super(builder);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            Arrays.stream(handlers).forEach( i-> p.addLast(i));
        }

    }

    @Override
    protected void before() throws Throwable {
        AbstractHttpServer.Builder<CustomServer> builder = new AbstractHttpServer.Builder<CustomServer>() {

            @Override
            public CustomServer build() {
                return new CustomServer(this);
            }

        };
        server = builder.setPort(this.port).setSSLContext(ssl).useSSL(ssl != null).build();
        server.configure(server);
    }

    @Override
    protected void after() {
        server.close();
    }
}
