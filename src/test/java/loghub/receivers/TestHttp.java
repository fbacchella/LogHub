package loghub.receivers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.security.JWTHandler;
import loghub.security.ssl.ContextLoader;

public class TestHttp {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Http", "loghub.netty", "loghub.EventsProcessor", "loghub.security", "loghub.netty.http");
    }

    private Http receiver = null;
    private BlockingQueue<Event> queue;
    private String hostname;
    private int port;

    public Http makeReceiver(Consumer<Http> prepare, Map<String, Object> propsMap) throws IOException {
        // Generate a locally binded random socket
        ServerSocket socket = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
        hostname = socket.getInetAddress().getHostAddress();
        port = socket.getLocalPort();
        socket.close();

        queue = new ArrayBlockingQueue<>(1);
        receiver = new Http(queue, new Pipeline(Collections.emptyList(), "testhttp", null));
        receiver.setHost(hostname);
        receiver.setPort(port);
        receiver.setDecoder(new StringCodec());
        prepare.accept(receiver);
        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
        return receiver;
    }

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private final String[] doRequest(URL destination, byte[] postDataBytes, Consumer<HttpURLConnection> prepare, int expected) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) destination.openConnection();
        if (conn instanceof HttpsURLConnection) {
            HttpsURLConnection cnx = (HttpsURLConnection) conn;
            cnx.setHostnameVerifier(new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
            Map<String, Object> properties = new HashMap<>();
            properties.put("trusts", new String[] {getClass().getResource("/localhost.p12").getFile()});
            SSLContext cssctx = ContextLoader.build(properties);
            cnx.setSSLSocketFactory(cssctx.getSocketFactory());
        }
        prepare.accept(conn);
        if (postDataBytes.length > 0) {
            conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
            conn.setDoOutput(true);
            conn.getOutputStream().write(postDataBytes);
        }
        String[] result = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8")).lines().toArray(String[]::new);
        Assert.assertEquals(expected, conn.getResponseCode());
        return result;
    }

    @Test
    public void testHttpPostJson() throws IOException {
        try (Http receiver = makeReceiver( i -> {}, Collections.emptyMap())) {
            doRequest(new URL("http", hostname, port, "/"),
                    "{\"a\": 1}".getBytes("UTF-8"),
                    i -> {
                        try {
                            i.setRequestMethod("PUT");
                            i.setRequestProperty("Content-Type", "application/json");
                        } catch (ProtocolException e1) {
                            throw new UncheckedIOException(e1);
                        }
                    }, 200);
            Event e = queue.poll();
            logger.debug(e.getClass());
            Integer a = (Integer) e.get("a");
            Assert.assertEquals(1, a.intValue());
        }
    }

    @Test
    public void testHttpGet() throws IOException {
        makeReceiver( i -> {}, Collections.emptyMap());
        doRequest(new URL("http", hostname, port, "/?a=1"),
                new byte[]{},
                i -> {}, 200);

        Event e = queue.poll();
        logger.debug(e.getClass());
        String a = (String) e.get("a");
        Assert.assertEquals("1", a);
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
    }

    @Test
    public void testHttpsGet() throws IOException {
        makeReceiver( i -> { i.setWithSSL(true); i.setSSLClientAuthentication("REQUIRED");},
                Collections.singletonMap("ssl.trusts", new String[] {getClass().getResource("/localhost.p12").getFile()})
                );
        doRequest(new URL("https", hostname, port, "/?a=1"),
                new byte[]{},
                i -> {}, 200);

        Event e = queue.poll();
        logger.debug(e.getClass());
        String a = (String) e.get("a");
        Assert.assertEquals("1", a);
        Assert.assertEquals("CN=localhost", e.getConnectionContext().getPrincipal().toString());
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        // Test that ssl state is still good
        doRequest(new URL("https", hostname, port, "/?a=1"),
                new byte[]{},
                i -> {}, 200);
    }

    @Test
    public void testHttpFull() throws IOException {
        makeReceiver( i -> {}, Collections.emptyMap());
        try {
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 200);
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 200);
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 200);
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 200);
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 200);
        } catch (IOException e) {
            Assert.assertEquals("Server returned HTTP response code: 429 for URL: http://127.0.0.1:" + receiver.getPort() + "/?a=1", e.getMessage());
            return;
        }
    }

    @Test
    public void testHttpPostForm() throws IOException {
        makeReceiver( i -> {}, Collections.emptyMap());
        doRequest(new URL("http", hostname, port, "/"),
                "a=1&b=c%20d".getBytes("UTF-8"),
                i -> {
                    try {
                        i.setRequestMethod("POST");
                        i.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                    } catch (ProtocolException e1) {
                        throw new UncheckedIOException(e1);
                    }
                }, 200);
        Event e = queue.poll();
        Assert.assertEquals("1", e.get("a"));
        Assert.assertEquals("c d", e.get("b"));
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
    }

    @Test
    public void testFailedAuthentication1() throws IOException {
        try {
            makeReceiver( i -> { i.setUser("user") ; i.setPassword("password");}, Collections.emptyMap());
            doRequest(new URL("http", hostname, port, "/?a=1"),
                    new byte[]{},
                    i -> {}, 401);
        } catch (IOException e) {
            Assert.assertEquals("Server returned HTTP response code: 401 for URL: http://127.0.0.1:" + receiver.getPort() + "/?a=1", e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testFailedAuthentication2() throws IOException {
        try {
            makeReceiver( i -> { i.setUser("user") ; i.setPassword("password");}, Collections.emptyMap());
            URL dest = new URL("http", hostname, port, "/?a=1");
            doRequest(dest,
                    new byte[]{},
                    i -> {
                        String authStr = Base64.getEncoder().encodeToString("user:badpassword".getBytes());
                        i.setRequestProperty("Authorization", "Basic " + authStr);
                    }, 401);
        } catch (IOException e) {
            Assert.assertEquals("Server returned HTTP response code: 401 for URL: http://127.0.0.1:" + receiver.getPort() + "/?a=1", e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testGoodPasswordAuthentication() throws IOException {
        makeReceiver( i -> { i.setUser("user") ; i.setPassword("password");}, Collections.emptyMap());
        URL dest = new URL("http", hostname, port, "/?a=1");
        doRequest(dest,
                new byte[]{},
                i -> {
                    String authStr = Base64.getEncoder().encodeToString("user:password".getBytes());
                    i.setRequestProperty("Authorization", "Basic " + authStr);
                }, 200);
        Event e = queue.poll();
        Assert.assertEquals("1", e.get("a"));
        Assert.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
    }

    @Test
    public void testGoodJwtAuthentication() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put("jwt.alg", "HMAC256");
        String secret = UUID.randomUUID().toString();
        props.put("jwt.secret", secret);
        JWTHandler handler = JWTHandler.getBuilder().secret(secret).setAlg("HMAC256").build();
        String jwtToken = handler.getToken(new JMXPrincipal("user"));
        makeReceiver( i -> { i.setUseJwt(true); }, props);
        URL dest = new URL("http", hostname, port, "/?a=1");
        doRequest(dest,
                new byte[]{},
                i -> {
                    i.setRequestProperty("Authorization", "Bearer " + jwtToken);
                }, 200);
        Event e = queue.poll();
        Assert.assertEquals("1", e.get("a"));
        Assert.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
    }

    @Test
    public void testGoodJwtAuthenticationAsPassword() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put("jwt.alg", "HMAC256");
        String secret = UUID.randomUUID().toString();
        props.put("jwt.secret", secret);
        JWTHandler handler = JWTHandler.getBuilder().secret(secret).setAlg("HMAC256").build();
        String jwtToken = handler.getToken(new JMXPrincipal("user"));
        makeReceiver( i -> { i.setUseJwt(true); }, props);
        URL dest = new URL("http", hostname, port, "/?a=1");
        doRequest(dest,
                new byte[]{},
                i -> {
                    String authStr = Base64.getEncoder().encodeToString((":" + jwtToken).getBytes());
                    i.setRequestProperty("Authorization", "Basic " + authStr);
                }, 200);
        Event e = queue.poll();
        Assert.assertEquals("1", e.get("a"));
        Assert.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
    }

}
