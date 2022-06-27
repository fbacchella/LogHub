package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.ContextLoader;

public class TestHttpSsl {

    @ContentType("text/plain")
    @RequestAccept(path="/")
    static class SimpleHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
            ByteBuf content = Unpooled.copiedBuffer("Request received\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }

    }

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer", "loghub.netty");
        Configurator.setLevel("org", Level.WARN);
    }

    Function<Map<String, Object> , SSLContext> getContext = (props) -> {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.2");
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.putAll(props);
        SSLContext newCtxt = ContextLoader.build(null, properties);
        Assert.assertEquals("TLSv1.2", newCtxt.getProtocol());
        return newCtxt;
    };

    private final int serverPort = Tools.tryGetPort();
    private final URL theurl;
    {
        try {
            theurl = new URL(String.format("https://localhost:%d/", serverPort));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpTestServer.CustomServer server;
    private void makeServer(Map<String, Object> sslprops, Consumer<HttpTestServer.CustomServer.Builder> c)
            throws IllegalArgumentException, InterruptedException, ExecutionException {
        HttpTestServer.CustomServer.Builder builder = new HttpTestServer.CustomServer.Builder();
        builder.setThreadPrefix("TestHttpSSL");
                        //builder.setConsumer(server)
        builder.setPort(serverPort);
        builder.setSslClientAuthentication(ClientAuthentication.REQUIRED);
        builder.setSslContext(getContext.apply(sslprops));
        builder.setSslKeyAlias("localhost (loghub ca)");
        builder.setWithSSL(true);

        c.accept(builder);
        server = builder.build();
        server.start();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.stop();
        }
        server = null;
    }

    @Test
    public void TestSimple() throws ExecutionException, InterruptedException, IOException {
        makeServer(Collections.emptyMap(), i -> {
            i.setHandlers(new HttpHandler[]{new SimpleHandler()});

        });
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestGoogle() throws IOException {
        URL google = new URL("https://www.google.com");
        HttpsURLConnection cnx = (HttpsURLConnection) google.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test
    public void TestClientAuthentication()
            throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(Collections.emptyMap(), i -> {
            i.setHandlers(new HttpHandler[]{new SimpleHandler()});
            i.setSslClientAuthentication(ClientAuthentication.REQUIRED);
        });
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test(expected=IOException.class)
    public void TestClientAuthenticationFailed()
            throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.singletonMap("issuers", new String[] {"cn=notlocalhost"})).getSocketFactory());
        cnx.connect();
        cnx.disconnect();
    }

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestChangedAlias()
            throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        makeServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test(expected=SocketException.class)
    public void TestNoSsl() throws IOException, IllegalArgumentException, InterruptedException,
                                           ExecutionException {
        makeServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        URL nohttpsurl = new URL("http", theurl.getHost(), theurl.getPort(), theurl.getFile());
        HttpURLConnection cnx = (HttpURLConnection) nohttpsurl.openConnection();
        // This generate two log lines, HttpURLConnection retry the connection
        cnx.connect();
        cnx.getResponseCode();
        cnx.disconnect();
    }

}
