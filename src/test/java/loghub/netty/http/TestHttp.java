package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import loghub.HttpTestServer;
import loghub.HttpTestServer.CustomServer.Builder;
import loghub.Tools;

public class TestHttp {

    @Rule
    public ExternalResource resource = getHttpServer();

    private ExternalResource getHttpServer() {
        serverPort = Tools.tryGetPort();
        return new HttpTestServer(null, serverPort);
    }

    private int serverPort = Tools.tryGetPort();
    private final URL theurl;
    {
        try {
            theurl = new URL(String.format("http://localhost:%d/", serverPort));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
    private HttpTestServer.CustomServer server;
    private void makeServer(Map<String, Object> sslprops, Function<Builder, Builder> c) {
        Builder builder = new Builder();
        builder.setThreadPrefix("TestHttp");
        builder.setPort(serverPort);
        builder.setWithSSL(false);
        server = c.apply(builder).build();
    }

    @Test
    public void Test404() throws IOException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i);
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        Assert.assertEquals(404, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test
    public void Test503() throws IOException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> {
            HttpRequestProcessing processing = new HttpRequestProcessing() {
                @Override
                public boolean acceptRequest(HttpRequest request) {
                    return true;
                }

                @Override
                protected void processRequest(FullHttpRequest request,
                                                 ChannelHandlerContext ctx)
                                                                 throws HttpRequestFailure {
                    throw new RuntimeException();
                }
            };
            i.setHandlers(new HttpHandler[]{processing});
            return i;
        });
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        Assert.assertEquals(503, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test
    public void TestRootRedirect() throws IOException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> {
            HttpRequestProcessing processing = new RootRedirect();
            i.setHandlers(new HttpHandler[]{processing});
            return i;
        });
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(true);
        Assert.assertEquals(404, cnx.getResponseCode());
        Assert.assertTrue(cnx.getURL().toString().endsWith("/static/index.html"));
        cnx.disconnect();
    }

}
