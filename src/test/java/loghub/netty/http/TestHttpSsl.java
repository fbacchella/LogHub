package loghub.netty.http;

import java.io.IOException;
import java.net.MalformedURLException;
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
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            ByteBuf content = Unpooled.copiedBuffer("Request received\r\n", CharsetUtil.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        }

    }

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer");
        Configurator.setLevel("org", Level.WARN);
    }

    Function<Map<String, Object> , SSLContext> getContext = (props) -> {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.2");
        properties.put("trusts", new String[] {getClass().getResource("/localhost.p12").getFile(), "/Users/fa4/Documents/Exalead/svn/Prod/Puppet/modules/jdk/files/alldsca.jks"});
        properties.putAll(props);
        SSLContext newCtxt = ContextLoader.build(properties);
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
            public CustomServer build() {
                return new CustomServer(this);
            }
        }

        protected CustomServer(Builder builder) {
            super(builder);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addLast(new SimpleHandler());
        }

    }

    private CustomServer server;
    private void makeServer(Map<String, Object> sslprops, Function<CustomServer.Builder, CustomServer.Builder> c) {
        CustomServer.Builder builder = new CustomServer.Builder().setPort(serverPort).setSSLContext(getContext.apply(sslprops)).useSSL(true);
        server = c.apply(builder).build();
        server.configure(server);
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.close();
        }
        server = null;
    }

    @Test
    public void TestSimple() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        makeServer(Collections.emptyMap(), i -> i);
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
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
    }

    @Test
    public void TestClientAuthentication() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        makeServer(Collections.emptyMap(), i -> i.setSSLClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
    }

    @Test(expected=IOException.class)
    public void TestClientAuthenticationFailed() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        makeServer(Collections.emptyMap(), i -> i.setSSLClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.singletonMap("issuers", new String[] {"cn=notlocalhost"})).getSocketFactory());
        cnx.connect();
    }

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestChangedAlias() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
        makeServer(Collections.emptyMap(), i -> i.setSSLKeyAlias("invalidalias"));
        HttpsURLConnection cnx = (HttpsURLConnection) theurl.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try(Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
    }

}
