package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import loghub.Tools;

public class TestHttp {

    private static class CustomServer extends AbstractHttpServer<CustomServer, CustomServer.Builder> {
        private static class Builder extends AbstractHttpServer.Builder<CustomServer, Builder> {
            ChannelHandler customHandler;
            @Override
            public CustomServer build() throws IllegalArgumentException, InterruptedException {
                return new CustomServer(this);
            }
        }
        private final ChannelHandler customHandler;
        protected CustomServer(Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
            this.customHandler = builder.customHandler;
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addLast(customHandler);
        }

    }

    private final int serverPort = Tools.tryGetPort();
    private final URL theurl;
    {
        try {
            theurl = new URL(String.format("http://localhost:%d/", serverPort));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
    private CustomServer server;
    private void makeServer(Map<String, Object> sslprops, Function<CustomServer.Builder, CustomServer.Builder> c) throws IllegalArgumentException, InterruptedException {
        CustomServer.Builder builder = new CustomServer.Builder()
                        .setThreadPrefix("TestHttp")
                        .setConsumer(server)
                        .setPort(serverPort)
                        .useSSL(false);

        server = c.apply(builder).build();
    }

    @After
    public void close() {
        if (server != null) {
            server.close();
        }
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
            i.customHandler = new HttpRequestProcessing() {
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
            return i;
        });
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        Assert.assertEquals(503, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test
    public void TestRootRedirect() throws IOException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> {
            i.customHandler = new RootRedirect();
            return i;
        });
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.setInstanceFollowRedirects(true);
        Assert.assertEquals(404, cnx.getResponseCode());
        Assert.assertTrue(cnx.getURL().toString().endsWith("/static/index.html"));
        cnx.disconnect();
    }

}
