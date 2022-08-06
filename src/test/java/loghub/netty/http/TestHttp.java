package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.transport.TransportConfig;

public class TestHttp {

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    @Rule
    public final HttpTestServer resource = new HttpTestServer();
    public final URL resourceUrl = resource.startServer(new TransportConfig());

    @Test(timeout = 1000)
    public void Test404() throws IOException, IllegalArgumentException {
        HttpURLConnection cnx = (HttpURLConnection) resourceUrl.openConnection();
        Assert.assertEquals(404, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test(timeout = 1000)
    public void Test503() throws IOException, IllegalArgumentException {
        HttpRequestProcessing processing = new HttpRequestProcessing() {
            @Override
            public boolean acceptRequest(HttpRequest request) {
                return true;
            }

            @Override
            protected void processRequest(FullHttpRequest request,
                    ChannelHandlerContext ctx) {
                throw new RuntimeException();
            }
        };
        resource.setModelHandlers(processing);
        HttpURLConnection cnx = (HttpURLConnection) resourceUrl.openConnection();
        Assert.assertEquals(503, cnx.getResponseCode());
        cnx.disconnect();
    }

    @Test(timeout = 1000)
    public void TestRootRedirect()
            throws IOException, IllegalArgumentException {
        resource.setModelHandlers(new RootRedirect());
        HttpURLConnection cnx = (HttpURLConnection) resourceUrl.openConnection();
        cnx.setInstanceFollowRedirects(true);
        Assert.assertEquals(404, cnx.getResponseCode());
        Assert.assertTrue(cnx.getURL().toString().endsWith("/static/index.html"));
        cnx.disconnect();
    }

}
