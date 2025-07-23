package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.transport.TcpTransport;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

public class TestHttpSsl {

    @ContentType("text/plain")
    @RequestAccept(path = "/")
    static class SimpleHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
            ByteBuf content = Unpooled.copiedBuffer("Request received\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }

    }

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer", "loghub.netty");
        Configurator.setLevel("org", Level.WARN);
    }

    final Function<Map<String, Object>, SSLContext> getContext = props -> {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.2");
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.putAll(props);
        SSLContext newCtxt = SslContextBuilder.getBuilder(properties).build();
        Assert.assertEquals("TLSv1.2", newCtxt.getProtocol());
        return newCtxt;
    };

    @Rule
    public final HttpTestServer resource = new HttpTestServer();

    private URL startHttpServer(Map<String, Object> sslprops, Consumer<TcpTransport.Builder> postconfig) {
        TcpTransport.Builder config = TcpTransport.getBuilder();
        config.setEndpoint("localhost");
        config.setWithSsl(true);
        config.setSslContext(getContext.apply(sslprops));
        config.setSslKeyAlias("localhost (loghub ca)");
        config.setSslClientAuthentication(ClientAuthentication.REQUIRED);
        config.setThreadPrefix("TestHttpSSL");
        postconfig.accept(config);
        return resource.startServer(config);
    }

    @Test
    public void testSimple() throws IOException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> { });
        resource.setModelHandlers(new SimpleHandler());
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try (Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test
    public void testHttpClient() throws IOException, InterruptedException, URISyntaxException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> { });
        resource.setModelHandlers(new SimpleHandler());
        HttpClient client = HttpClient.newBuilder()
                                    .sslContext(getContext.apply(Collections.emptyMap()))
                                    .build();

        HttpRequest request = HttpRequest.newBuilder()
                                      .uri(theURL.toURI())
                                      .GET()
                                      .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertNotNull(response.body());
        Assert.assertEquals(200, response.statusCode());
        Assert.assertEquals("CN=localhost", response.sslSession().get().getPeerPrincipal().getName());
    }

    @Test
    public void testGoogle() throws IOException, URISyntaxException {
        URL google = new URI("https://www.google.com").toURL();
        HttpsURLConnection cnx = (HttpsURLConnection) google.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        try {
            Assert.assertThrows(SSLHandshakeException.class, cnx::connect);
        } finally {
            cnx.disconnect();
        }
    }

    @Test
    public void testClientAuthentication()
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.REQUIRED));
        resource.setModelHandlers(new SimpleHandler());
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try (Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test
    public void testClientAuthenticationFailed()
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.singletonMap("issuers", new String[] {"cn=notlocalhost"})).getSocketFactory());
        try {
            Assert.assertThrows(SocketException.class, cnx::connect);
        } finally {
            cnx.disconnect();
        }
    }

    @Test
    public void testChangedAlias()
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        try {
            Assert.assertThrows(SSLHandshakeException.class, cnx::connect);
        } finally {
            cnx.disconnect();
        }
    }

    @Test
    public void testNoSsl() throws IOException, IllegalArgumentException, URISyntaxException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        URL nohttpsurl = new URI("http", null, theURL.getHost(), theURL.getPort(), theURL.getFile(), null, null).toURL();
        HttpURLConnection cnx = (HttpURLConnection) nohttpsurl.openConnection();
        // This generate two log lines, HttpURLConnection retry the connection
        cnx.connect();
        try {
            Assert.assertThrows(SocketException.class, cnx::getResponseCode);
        } finally {
            cnx.disconnect();
        }
    }

}
