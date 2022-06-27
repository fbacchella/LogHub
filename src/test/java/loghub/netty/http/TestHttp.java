package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import loghub.HttpTestServer;
import loghub.HttpTestServer.CustomServer.Builder;
import loghub.LogUtils;
import loghub.Tools;

public class TestHttp {

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    private final int serverPort = Tools.tryGetPort();
    private final URL theurl;
    {
        try {
            theurl = new URL(String.format("http://localhost:%d/", serverPort));
            System.err.println(theurl);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
    private HttpTestServer.CustomServer server;
    private void makeServer(Function<Builder, Builder> c)
            throws ExecutionException, InterruptedException {
        Builder builder = new Builder();
        builder.setThreadPrefix("TestHttp");
        builder.setPort(serverPort);
        builder.setWithSSL(false);
        server = c.apply(builder).build();
        server.start();
    }

    @After
    public void stopServer() throws ExecutionException, InterruptedException {
        if (server != null) {
            server.stop();
        }
        server = null;
    }

    @Test(timeout = 1000)
    public void Test404() throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(i -> i);
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        Assert.assertEquals(404, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test(timeout = 1000)
    public void Test503() throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(i -> {
            HttpRequestProcessing processing = new HttpRequestProcessing() {
                @Override
                public boolean acceptRequest(HttpRequest request) {
                    Thread.dumpStack();return true;
                }

                @Override
                protected void processRequest(FullHttpRequest request,
                                                 ChannelHandlerContext ctx) {
                    Thread.dumpStack();
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

    @Test(timeout = 1000)
    public void TestRootRedirect()
            throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(i -> {
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
