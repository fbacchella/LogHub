package loghub.netty.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.HttpTestServer;
import loghub.Tools;
import loghub.netty.http2.Http2Handler;
import loghub.netty.http2.Http2RequestProcessing;
import loghub.netty.transport.TcpTransport;
import loghub.security.ssl.SslContextBuilder;

public class TestHttp2Handler {

    private HttpTestServer resource;
    private SSLContext sslContext;

    @BeforeEach
    public void setup() throws Exception {
        resource = new HttpTestServer();
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.3");
        properties.put("trusts", Tools.getDefaultKeyStore());
        sslContext = SslContextBuilder.getBuilder(properties).build();
    }

    @AfterEach
    public void teardown() {
        resource.after();
    }

    @RequestAccept(path = "/test")
    @ContentType("text/plain")
    @NoCache
    private static class SimpleHttp2Handler extends Http2RequestProcessing {
        @Override
        protected void processRequest(Http2HeadersFrame frame, ChannelHandlerContext ctx) throws HttpRequestFailure {
            writeResponse(ctx, frame, Unpooled.copiedBuffer("Hello HTTP/2", StandardCharsets.UTF_8), 12);
        }
    }

    @Test
    @Timeout(10)
    public void testHttp2Handler() throws IOException, InterruptedException {
        TcpTransport.Builder builder = TcpTransport.getBuilder();
        builder.setWithSsl(true);
        builder.setSslContext(sslContext);
        builder.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
        builder.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);

        SimpleHttp2Handler handler = new SimpleHttp2Handler();
        resource.setHttp2handler(ch -> ch.pipeline().addLast(handler));
        URI uri = resource.startServer(builder);

        HttpClient client = HttpClient.newBuilder()
                                      .sslContext(sslContext)
                                      .version(HttpClient.Version.HTTP_2)
                                      .build();

        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(uri.resolve("/test"))
                                         .GET()
                                         .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("Hello HTTP/2", response.body());
        Assertions.assertEquals("text/plain", response.headers().firstValue("Content-Type").orElse(null));
        Assertions.assertEquals("private, max-age=0", response.headers().firstValue("Cache-Control").orElse(null));
        Assertions.assertTrue(response.headers().firstValue("Last-Modified").isPresent());
    }

    @Test
    @Timeout(10)
    public void testHttp2HandlerFailure() throws IOException, InterruptedException {
        TcpTransport.Builder builder = TcpTransport.getBuilder();
        builder.setWithSsl(true);
        builder.setSslContext(sslContext);
        builder.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);

        Http2Handler handler = new Http2Handler() {
            @Override
            protected void subProcessing(Http2HeadersFrame frame, ChannelHandlerContext ctx) throws HttpRequestFailure {
                throw new HttpRequestFailure(io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST, "Failed");
            }
            @Override
            public boolean acceptRequest(io.netty.handler.codec.http2.Http2Headers headers) {
                return true;
            }
        };
        resource.setHttp2handler(ch -> ch.pipeline().addLast(handler));
        URI uri = resource.startServer(builder);

        HttpClient client = HttpClient.newBuilder()
                                      .sslContext(sslContext)
                                      .version(HttpClient.Version.HTTP_2)
                                      .build();

        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(uri.resolve("/fail"))
                                         .GET()
                                         .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("Failed\r\n", response.body());
    }
}
