package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.decoders.Decoder;
import loghub.decoders.Json;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;
import loghub.types.MimeType;

class TestHttp {

    private static Logger logger;
    private static String p12File;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Http", "loghub.netty", "loghub.EventsProcessor", "loghub.security");
        p12File = Optional.ofNullable(TestHttp.class.getResource("/loghub.p12"))
                          .map(URL::getFile)
                          .orElseThrow();
        JmxService.start(JmxService.configuration());
    }

    private Http receiver = null;
    private PriorityBlockingQueue queue;
    private String hostname;
    private int port;
    private URL testURL;

    public Http makeReceiver(Consumer<Http.Builder> prepare, Map<String, Object> propsMap)
            throws URISyntaxException, MalformedURLException {
        // Generate a locally bound random socket
        port = Tools.tryGetPort();
        hostname = InetAddress.getLoopbackAddress().getCanonicalHostName();
        testURL = new URI("http", null, hostname, port, "/", "a=1", null).toURL();
        queue = new PriorityBlockingQueue();

        Properties props = new Properties(propsMap);
        Json.Builder builder = Json.getBuilder();
        builder.setCharset("UTF-8");
        Json jdec = builder.build();
        jdec.configure(null, receiver);

        Http.Builder httpbuilder = Http.getBuilder();
        httpbuilder.setDecoders(Collections.singletonMap("application/json", jdec));
        httpbuilder.setHost(hostname);
        httpbuilder.setPort(port);
        httpbuilder.setEventsFactory(factory);
        prepare.accept(httpbuilder);

        receiver = httpbuilder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testhttp", null));
        Assertions.assertTrue(receiver.configure(props));
        Stats.registerReceiver(receiver);
        receiver.start();
        return receiver;
    }

    @AfterEach
    void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private void doRequest(URI destination, String method, HttpRequest.BodyPublisher bodyPublisher, Consumer<HttpRequest.Builder> prepare, int expected, HttpClient.Version version) throws IOException, InterruptedException {
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        if ("https".equals(destination.getScheme())) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("trusts", p12File);
            properties.put("issuers", new String[]{"CN=loghub CA"});
            SSLContext cssctx = SslContextBuilder.getBuilder(null, properties).build();
            clientBuilder.sslContext(cssctx);
        }
        clientBuilder.version(version);
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(destination)
                    .method(method, bodyPublisher);
            prepare.accept(requestBuilder);
            HttpResponse<String> response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(expected, response.statusCode());
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testHttpPostJson(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        try (Http ignored = makeReceiver(i -> { }, Collections.emptyMap())) {
            doRequest(new URI("http", null, hostname, port, "/", null, null),
                      "PUT",
                      HttpRequest.BodyPublishers.ofString("{\"a\": 1}"),
                      i -> i.header("Content-Type", "application/json"),
                      200,
                      version);
            Event e = queue.poll();
            Assertions.assertNotNull(e);
            Integer a = (Integer) e.get("a");
            Assertions.assertEquals(1, a.intValue());
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testHttpGet(HttpClient.Version version) throws IOException, InterruptedException, URISyntaxException {
        try (Http ignored = makeReceiver(i -> { }, Collections.emptyMap())) {
            doRequest(testURL.toURI(),
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> { }, 200, version);

            Event e = queue.take();
            String a = (String) e.get("a");
            Assertions.assertEquals("1", a);
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            ConnectionContext<InetSocketAddress> ectxt = e.getConnectionContext();
            Assertions.assertNotNull(ectxt);
            Assertions.assertNotNull(ectxt.getLocalAddress());
            Assertions.assertNotNull(ectxt.getRemoteAddress());
        }
    }

    @ParameterizedTest
    @EnumSource(value = HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testHttpsGet(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        SSLContext sslctx = SslContextBuilder.getBuilder(getClass().getClassLoader(), new HashMap<>(Map.of("trusts", p12File))).build();
        try (Http ignored = makeReceiver(i -> {
            i.setSslContext(sslctx);
            i.setWithSSL(true);
            i.setSSLClientAuthentication(ClientAuthentication.WANTED);
        }, Collections.emptyMap())) {
            URI uri = new URI("https", null, hostname, port, "/", "a=1", null);
            doRequest(uri,
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> { }, 200, version);

            Event e = queue.poll();
            Assertions.assertNotNull(e);
            String a = (String) e.get("a");
            Assertions.assertEquals("1", a);
            Assertions.assertEquals("CN=localhost", e.getConnectionContext().getPrincipal().toString());
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            ConnectionContext<InetSocketAddress> ectxt = e.getConnectionContext();
            Assertions.assertNotNull(ectxt.getLocalAddress());
            Assertions.assertNotNull(ectxt.getRemoteAddress());
            // Test that ssl state is still good
            doRequest(uri,
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> { }, 200, version);
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testHttpPostForm(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        try (Http ignored = makeReceiver(i -> { }, Collections.emptyMap())) {
            doRequest(new URI("http", null, hostname, port, "/", null, null),
                    "POST",
                    HttpRequest.BodyPublishers.ofString("a=1&b=c%20d"),
                    i -> i.header("Content-Type", "application/x-www-form-urlencoded"),
                    200, version);
            Event e = queue.poll();
            Assertions.assertNotNull(e);
            Assertions.assertEquals("1", e.get("a"));
            Assertions.assertEquals("c d", e.get("b"));
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testFailedAuthentication1(HttpClient.Version version) throws IOException, InterruptedException, URISyntaxException {
        try (Http ignored = makeReceiver(i -> { i.setUser("user"); i.setPassword("password");}, Collections.emptyMap())) {
            doRequest(testURL.toURI(),
                      "GET",
                      HttpRequest.BodyPublishers.noBody(),
                      i -> { }, 401, version);
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testFailedAuthentication2(HttpClient.Version version) throws URISyntaxException, IOException, InterruptedException {
        try (Http ignored = makeReceiver(i -> { i.setUser("user"); i.setPassword("password");}, Collections.emptyMap())) {
            doRequest(testURL.toURI(),
                      "GET",
                      HttpRequest.BodyPublishers.noBody(),
                      i -> {
                          String authStr = Base64.getEncoder().encodeToString("user:badpassword".getBytes());
                          i.header("Authorization", "Basic " + authStr);
                      }, 401, version);
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGoodPasswordAuthentication(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        try (Http ignored = makeReceiver(i -> { i.setUser("user"); i.setPassword("password");}, Collections.emptyMap())) {
            doRequest(testURL.toURI(),
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> {
                        String authStr = Base64.getEncoder().encodeToString("user:password".getBytes());
                        i.header("Authorization", "Basic " + authStr);
                    }, 200, version);
            Event e = queue.poll();
            Assertions.assertNotNull(e);
            Assertions.assertEquals("1", e.get("a"));
            Assertions.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGoodJwtAuthentication(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put("jwt.alg", "HMAC256");
        String secret = UUID.randomUUID().toString();
        props.put("jwt.secret", secret);
        JWTHandler handler = JWTHandler.getBuilder().secret(secret).setAlg("HMAC256").build();
        String jwtToken = handler.getToken(new JMXPrincipal("user"));
        try (Http ignored = makeReceiver(i -> { i.setUseJwt(true); i.setJwtHandler(handler);}, props)) {
            URI dest = new URI("http", null, hostname, port, "/", "a=1", null);
            doRequest(dest,
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> i.header("Authorization", "Bearer " + jwtToken), 200, version);
            Event e = queue.poll();
            Assertions.assertNotNull(e);
            Assertions.assertEquals("1", e.get("a"));
            Assertions.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGoodJwtAuthenticationAsPassword(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put("jwt.alg", "HMAC256");
        String secret = UUID.randomUUID().toString();
        props.put("jwt.secret", secret);
        JWTHandler handler = JWTHandler.getBuilder().secret(secret).setAlg("HMAC256").build();
        String jwtToken = handler.getToken(new JMXPrincipal("user"));
        try (Http ignored = makeReceiver(i -> { i.setUseJwt(true); i.setJwtHandler(handler);}, props)) {
            URI dest = new URI("http", null, hostname, port, "/", "a=1", null);
            doRequest(dest,
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> {
                        String authStr = Base64.getEncoder().encodeToString((":" + jwtToken).getBytes());
                        i.header("Authorization", "Basic " + authStr);
                    }, 200, version);
            Event e = queue.poll();
            Assertions.assertNotNull(e);
            Assertions.assertEquals("1", e.get("a"));
            Assertions.assertEquals("user", e.getConnectionContext().getPrincipal().getName());
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void manyDecoders() throws ConfigException, IOException {
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
        Map<MimeType, Decoder> decs = http.getDecoders();
        Assertions.assertTrue(decs.containsKey(MimeType.of("application/csv")));
        Assertions.assertTrue(decs.containsKey(MimeType.of("application/msgpack")));
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void noExplicitDecoder() throws ConfigException, IOException {
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
            Assertions.assertEquals("Decoder loghub.decoders.Msgpack will be ignored, this receiver handle decoding", ex.getMessage());
            Assertions.assertEquals("file <unknown>, line 1:11", ex.getLocation());
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testFailedEncoder(HttpClient.Version version) throws IOException, URISyntaxException, InterruptedException {
        try (Http ignored = makeReceiver(i -> i.setDecoders(Collections.singletonMap("application/json", ReceiverTools.getFailingDecoder())), Collections.emptyMap())) {
            doRequest(testURL.toURI(),
                    "GET",
                    HttpRequest.BodyPublishers.noBody(),
                    i -> { }, 200, version
            );

            Event e = queue.poll();
            Assertions.assertNotNull(e);
            String a = (String) e.get("a");
            Assertions.assertEquals("1", a);
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            ConnectionContext<InetSocketAddress> ectxt = e.getConnectionContext();
            Assertions.assertNotNull(ectxt);
            Assertions.assertNotNull(ectxt.getLocalAddress());
            Assertions.assertNotNull(ectxt.getRemoteAddress());
        }
    }

    @Test
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Http"
                              , BeanInfo.build("decoders", Map.class)
                              , BeanInfo.build("useJwt", Boolean.TYPE)
                              , BeanInfo.build("user", String.class)
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("jaasName", String.class)
                              , BeanInfo.build("withSSL", Boolean.TYPE)
                              , BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                              , BeanInfo.build("SSLKeyAlias", String.class)
                              , BeanInfo.build("backlog", Integer.TYPE)
                              , BeanInfo.build("sndBuf", Integer.TYPE)
                              , BeanInfo.build("rcvBuf", Integer.TYPE)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
