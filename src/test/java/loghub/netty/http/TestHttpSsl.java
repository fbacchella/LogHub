package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
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
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import loghub.LogUtils;
import loghub.Tools;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.ContextLoader;

public class TestHttpSsl {

    @ContentType("text/plain")
    @RequestAccept(path="/")
    static class SimpleHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
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

    private static class CustomServer extends AbstractHttpServer<CustomServer, CustomServer.Builder> {
        private static class Builder extends AbstractHttpServer.Builder<CustomServer, Builder> {
            @Override
            public CustomServer build() throws IllegalArgumentException, InterruptedException {
                return new CustomServer(this);
            }
        }

        protected CustomServer(Builder builder) throws IllegalArgumentException, InterruptedException {
            super(builder);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addLast(new SimpleHandler());
        }

    }

    private CustomServer server;
    private void makeServer(Map<String, Object> sslprops, Function<CustomServer.Builder, CustomServer.Builder> c) throws IllegalArgumentException, InterruptedException {
        CustomServer.Builder builder = new CustomServer.Builder()
                        .setThreadPrefix("TestHttpSSL")
                        .setConsumer(server)
                        .setPort(serverPort)
                        .setSSLClientAuthentication(ClientAuthentication.REQUIRED)
                        .setSSLContext(getContext.apply(sslprops))
                        .useSSL(true);

        server = c.apply(builder).build();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.close();
        }
        server = null;
    }

    @Test
    public void TestSimple() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i);
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
    public void TestGoogle() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
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
    public void TestClientAuthentication() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i.setSSLClientAuthentication(ClientAuthentication.REQUIRED));
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
    public void TestClientAuthenticationFailed() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i.setSSLClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.singletonMap("issuers", new String[] {"cn=notlocalhost"})).getSocketFactory());
        cnx.connect();
        cnx.disconnect();
    }

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestChangedAlias() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i.setSSLKeyAlias("invalidalias"));
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
    public void TestNoSsl() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException, IllegalArgumentException, InterruptedException {
        makeServer(Collections.emptyMap(), i -> i.setSSLKeyAlias("invalidalias"));
        URL nohttpsurl = new URL("http", theurl.getHost(), theurl.getPort(), theurl.getFile());
        HttpURLConnection cnx = (HttpURLConnection) nohttpsurl.openConnection();
        // This generate two log lines, HttpURLConnection retry the connection
        cnx.connect();
        cnx.getResponseCode();
        cnx.disconnect();
    }

}
