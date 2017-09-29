package loghub;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.http.config.SocketConfig;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.junit.rules.ExternalResource;

public class HttpTestServer extends ExternalResource {

    public static class HandlerInfo extends AbstractMap.SimpleImmutableEntry<String, HttpRequestHandler> {
        public HandlerInfo(String key, HttpRequestHandler value) {
            super(key, value);
        }
    }

    private HttpServer server;
    private final Map.Entry<String, HttpRequestHandler>[] handlers;
    private SSLContext ssl;
    private int port;

    @SafeVarargs
    public HttpTestServer(SSLContext ssl, int port, HandlerInfo... handlers) {
        this.handlers = Arrays.copyOf(handlers, handlers.length);
        this.ssl = ssl;
        this.port = port;
    }

    @Override
    protected void before() throws Throwable {
        HttpProcessorBuilder builder = HttpProcessorBuilder.create()
                .add(new ResponseDate())
                .add(new ResponseServer("MyServer-HTTP/1.1"))
                .add(new ResponseContent())
                .add(new ResponseConnControl());
        HttpProcessor httpProcessor = builder.build();
        SocketConfig socketConfig = SocketConfig.custom()
                .setSoTimeout(15000)
                .setTcpNoDelay(true)
                .build();
        ServerBootstrap bootstrap = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .setHttpProcessor(httpProcessor)
                .setSocketConfig(socketConfig);
        if (ssl != null) {
            bootstrap.setSslContext(ssl);
        }
        Arrays.stream(handlers).forEach(i -> bootstrap.registerHandler(i.getKey(), i.getValue()));
        server =  bootstrap.create();
        server.start();
    }

    @Override
    protected void after() {
        server.stop();
    }
}
