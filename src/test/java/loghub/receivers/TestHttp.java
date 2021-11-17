package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;

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

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.decoders.Decoder;
import loghub.decoders.Json;
import loghub.security.JWTHandler;
import loghub.security.ssl.ContextLoader;

public class TestHttp {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.FATAL, "loghub.receivers.Http", "loghub.netty", "loghub.EventsProcessor", "loghub.security", "loghub.netty.http", "loghub.configuration");
    }

    private Http receiver = null;
    private PriorityBlockingQueue queue;
    private String hostname;
    private int port;

    public Http makeReceiver(Consumer<Http.Builder> prepare, Map<String, Object> propsMap) throws IOException {
        // Generate a locally binded random socket
        port = Tools.tryGetPort();
        hostname = InetAddress.getLoopbackAddress().getCanonicalHostName();

        queue = new PriorityBlockingQueue();

        Json.Builder builder = Json.getBuilder();
        builder.setCharset("UTF-8");
        Json jdec = builder.build();
        jdec.configure(null, receiver);

        Http.Builder httpbuilder = Http.getBuilder();
        httpbuilder.setDecoders(Collections.singletonMap("application/json", jdec));
        httpbuilder.setHost(hostname);
        httpbuilder.setPort(port);
        prepare.accept(httpbuilder);

        receiver = httpbuilder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testhttp", null));

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
                    logger.trace("Verifying {} with sessions {}", hostname, session);
                    return true;
                }
            });
            Map<String, Object> properties = new HashMap<>();
            properties.put("trusts", new String[] {getClass().getResource("/loghub.p12").getFile()});
            properties.put("issuers", new String[] {"CN=loghub CA"});
            SSLContext cssctx = ContextLoader.build(null, properties);
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
        conn.disconnect();
        return result;
    }

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
    public void testHttpGet() throws IOException {
        makeReceiver(i -> {}, Collections.emptyMap());
        doRequest(new URL("http", hostname, port, "/?a=1"),
                  new byte[]{},
                  i -> {}, 200);

        Event e = queue.poll();
        logger.debug(e.getClass());
        String a = (String) e.get("a");
        Assert.assertEquals("1", a);
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        ConnectionContext<InetSocketAddress> ectxt = e.getConnectionContext();
        Assert.assertNotNull(ectxt);
        Assert.assertTrue(ectxt.getLocalAddress() instanceof InetSocketAddress);
        Assert.assertTrue(ectxt.getRemoteAddress() instanceof InetSocketAddress);
        Assert.assertNull(((IpConnectionContext)ectxt).getSslParameters());
    }

    @Test(timeout = 5000)
    public void testHttpsGet() throws IOException {
        makeReceiver(i -> { 
            i.setWithSSL(true);
            i.setSSLClientAuthentication("WANTED");
        },
                     Collections.singletonMap("ssl.trusts", new String[] {getClass().getResource("/loghub.p12").getFile()})
                     );
        doRequest(new URL("https", hostname, port, "/?a=1"),
                  new byte[]{},
                  i -> {}, 200);

        Event e = queue.poll();
        String a = (String) e.get("a");
        Assert.assertEquals("1", a);
        Assert.assertEquals("CN=localhost", e.getConnectionContext().getPrincipal().toString());
        Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        ConnectionContext<InetSocketAddress> ectxt = e.getConnectionContext();
        Assert.assertTrue(ectxt.getLocalAddress() instanceof InetSocketAddress);
        Assert.assertTrue(ectxt.getRemoteAddress() instanceof InetSocketAddress);
        Assert.assertTrue(Pattern.matches("TLSv1.*", ((IpConnectionContext)ectxt).getSslParameters().getProtocol()));
        // Test that ssl state is still good
        doRequest(new URL("https", hostname, port, "/?a=1"),
                  new byte[]{},
                  i -> {}, 200);
    }

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
    public void testFailedAuthentication1() throws IOException {
        try {
            makeReceiver( i -> { i.setUser("user") ; i.setPassword("password");}, Collections.emptyMap());
            doRequest(new URL("http", hostname, port, "/?a=1"),
                      new byte[]{},
                      i -> {}, 401);
        } catch (IOException e) {
            Assert.assertEquals("Server returned HTTP response code: 401 for URL: http://" + hostname + ":" + receiver.getPort() + "/?a=1", e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test(timeout = 5000)
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
            Assert.assertEquals("Server returned HTTP response code: 401 for URL: http://" + hostname + ":" + receiver.getPort() + "/?a=1", e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
    public void manyDecoders() throws ConfigException, IOException {
        String confile = "input {" + 
                        "    loghub.receivers.Http {" + 
                        "        port: 1502," + 
                        "        decoders: {" + 
                        "            \"application/csv\": loghub.decoders.Json {" + 
                        "            }," + 
                        "            \"application/msgpack\": loghub.decoders.Msgpack {" + 
                        "            }," + 
                        "        }," + 
                        "    }" + 
                        "} | $main " + 
                        "pipeline[main] {" + 
                        "}";
        Properties conf = Tools.loadConf(new StringReader(confile));
        Http http = (Http) conf.receivers.toArray(new Receiver[1])[0];
        Map<String, Decoder> decs = http.getDecoders();
        Assert.assertTrue(decs.containsKey("application/csv"));
        Assert.assertTrue(decs.containsKey("application/msgpack"));
    }

    @Test(timeout = 5000)
    public void noExplicitDecoder() throws ConfigException, IOException {
        String confile = "input {" + 
                        "    loghub.receivers.Http {" + 
                        "        decoder: loghub.decoders.Msgpack {" + 
                        "        }," + 
                        "    }" + 
                        "} | $main";

        try {
            @SuppressWarnings("unused")
            Properties conf = Tools.loadConf(new StringReader(confile));
        } catch (ConfigException ex) {
            Assert.assertEquals("No default decoder can be defined", ex.getMessage());
            Assert.assertEquals("file <unknown>, line 1:11", ex.getLocation());
        }
    }

    @Test
    public void test_loghub_receivers_Http() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Http"
                              , BeanInfo.build("decoders", Map.class)
                              , BeanInfo.build("useJwt", Boolean.TYPE)
                              , BeanInfo.build("user", String.class)
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("jaasName", String.class)
                              , BeanInfo.build("withSSL", Boolean.TYPE)
                              , BeanInfo.build("SSLClientAuthentication", String.class)
                              , BeanInfo.build("SSLKeyAlias", String.class)
                              , BeanInfo.build("backlog", Integer.TYPE)
                              , BeanInfo.build("sndBuf", Integer.TYPE)
                              , BeanInfo.build("rcvBuf", Integer.TYPE)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
