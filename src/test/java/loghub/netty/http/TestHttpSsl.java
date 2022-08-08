package loghub.netty.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

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

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
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
    public void TestSimple() throws IOException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> {});
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

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestGoogle() throws IOException {
        URL google = new URL("https://www.google.com");
        HttpsURLConnection cnx = (HttpsURLConnection) google.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try (Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test
    public void TestClientAuthentication()
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> {
            i.setSslClientAuthentication(ClientAuthentication.REQUIRED);
        });
        resource.setModelHandlers(new SimpleHandler());
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
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
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.REQUIRED));
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.singletonMap("issuers", new String[] {"cn=notlocalhost"})).getSocketFactory());
        cnx.connect();
        cnx.disconnect();
    }

    @Test(expected=javax.net.ssl.SSLHandshakeException.class)
    public void TestChangedAlias()
            throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        HttpsURLConnection cnx = (HttpsURLConnection) theURL.openConnection();
        cnx.setSSLSocketFactory(getContext.apply(Collections.emptyMap()).getSocketFactory());
        cnx.connect();
        Assert.assertEquals("CN=localhost", cnx.getPeerPrincipal().getName());
        try (Scanner s = new Scanner(cnx.getInputStream())) {
            s.skip(".*");
        }
        cnx.disconnect();
    }

    @Test(expected=SocketException.class)
    public void TestNoSsl() throws IOException, IllegalArgumentException {
        URL theURL = startHttpServer(Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        URL nohttpsurl = new URL("http", theURL.getHost(), theURL.getPort(), theURL.getFile());
        HttpURLConnection cnx = (HttpURLConnection) nohttpsurl.openConnection();
        // This generate two log lines, HttpURLConnection retry the connection
        cnx.connect();
        cnx.getResponseCode();
        cnx.disconnect();
    }

}
