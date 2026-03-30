package loghub.netty.http;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.util.CharsetUtil;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.netty.http2.Http2RequestProcessing;
import loghub.netty.transport.TcpTransport;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

class TestHttpChannelConsumer {

    @ContentType("text/plain")
    @RequestAccept(path = "/")
    static class SimpleHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
            ByteBuf content = ctx.alloc().buffer();
            content.writeCharSequence("Request received\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }
    }

    @ContentType("text/plain")
    @RequestAccept(path = "/")
    static class SimpleHttp2Handler extends Http2RequestProcessing {
        @Override
        protected void processRequest(Http2HeadersFrame request, ChannelHandlerContext ctx) {
            ByteBuf content = ctx.alloc().buffer();
            content.writeCharSequence("HTTP2 Request received\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        }
    }

    @BeforeAll
    static void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer", "loghub.netty", "io.netty");
        Configurator.setLevel("org", Level.WARN);
        JmxService.start(JmxService.configuration());
    }

    final Function<Map<String, Object>, SSLContext> getContext = props -> {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSv1.3");
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.putAll(props);
        SSLContext newCtxt = SslContextBuilder.getBuilder(properties).build();
        Assertions.assertEquals("TLSv1.3", newCtxt.getProtocol());
        return newCtxt;
    };

    private final HttpTestServer resource = new HttpTestServer();

    @AfterEach
    void clean() {
        resource.after();
    }

    @BeforeEach
    void webStats() {
        Stats.reset();
        Stats.registerHttpService(resource.getHolder());
    }

    static Stream<Arguments> protocolArguments() {
        return Stream.of(
                Arguments.of(Version.HTTP_1_1, "http"),
                Arguments.of(Version.HTTP_1_1, "https"),
                Arguments.of(Version.HTTP_2, "http"),
                Arguments.of(Version.HTTP_2, "https")
        );
    }

    private URI startHttpServer(String scheme, Map<String, Object> sslprops, Consumer<TcpTransport.Builder> postconfig) {
        TcpTransport.Builder config = TcpTransport.getBuilder();
        config.setEndpoint("localhost");
        config.setSslContext(getContext.apply(sslprops));
        config.setSslKeyAlias("localhost (loghub ca)");
        config.setSslClientAuthentication(ClientAuthentication.REQUIRED);
        config.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
        config.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
        config.setThreadPrefix("TestHttpSSL");
        postconfig.accept(config);
        return resource.startServer(scheme, config);
    }

    private <T> void runRequest(HttpClient.Version version, URI url, BodyHandler<T> handler, Consumer<HttpResponse<T>> processResponse, HttpHandler... handlers)
            throws IOException, InterruptedException {
        String scheme = url.getScheme();
        resource.setModelHandlers(handlers);
        HttpClient.Builder clientBuilder = HttpClient.newBuilder().version(version);
        if ("https".equals(scheme)) {
            clientBuilder.sslContext(getContext.apply(Collections.emptyMap()));
        }
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(url)
                                          .GET()
                                          .build();
            HttpResponse<T> response = client.send(request, handler);
            processResponse.accept(response);
        }
    }

    private <T> void runRequest(HttpClient.Builder clientBuilder, URI url, BodyHandler<T> handler, Consumer<HttpResponse<T>> processResponse)
            throws IOException, InterruptedException {
        String scheme = url.getScheme();
        if ("https".equals(scheme)) {
            clientBuilder.sslContext(getContext.apply(Collections.emptyMap()));
        }
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(url)
                                          .GET()
                                          .build();
            HttpResponse<T> response = client.send(request, handler);
            processResponse.accept(response);
        }
    }

    private void checkTlsPeer(HttpResponse<?> response, String expected) {
        try {
            if ("https".equals(response.uri().getScheme())) {
                Assertions.assertEquals(expected, response.sslSession().orElseThrow().getPeerPrincipal().getName());
            }
        } catch (SSLPeerUnverifiedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testSimple(HttpClient.Version version, String scheme)
            throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertEquals("Request received\r\n", r.body());
            Assertions.assertEquals(200, r.statusCode());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse, new SimpleHandler());
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testSimpleAutoVersion(HttpClient.Version version, String scheme) throws IOException, InterruptedException {
        resource.setVersionedModelSetup((v, p) -> {
            switch (v) {
                case HTTP_1_1 ->
                    p.addLast(new SimpleHandler());
                case HTTP_2 ->
                    p.addLast(new SimpleHttp2Handler());
                default ->
                    throw new IllegalArgumentException("Unsupported HTTP version: " + v);
            }
        });
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            switch (r.version()) {
            case HTTP_1_1 ->
                    Assertions.assertEquals("Request received\r\n", r.body());
            case HTTP_2 ->
                    Assertions.assertEquals("HTTP2 Request received\r\n", r.body());
            default ->
                    throw new IllegalArgumentException("Unsupported HTTP version: " + r.version());
            }
            Assertions.assertEquals(200, r.statusCode());
        };
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                                                   .version(version);
        runRequest(clientBuilder, theURL, HttpResponse.BodyHandlers.ofString(), processResponse);
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void test503(HttpClient.Version version, String scheme) throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertEquals("Critical internal server error\r\n", r.body());
            Assertions.assertEquals(503, r.statusCode());
            Assertions.assertEquals("text/plain; charset=UTF-8", r.headers().firstValue("Content-Type").orElseThrow());
            checkTlsPeer(r, "CN=localhost");
        };
        HttpRequestProcessing processing = new HttpRequestProcessing() {
            @Override
            public boolean acceptRequest(io.netty.handler.codec.http.HttpRequest request) {
                return true;
            }

            @Override
            protected void processRequest(FullHttpRequest request,
                    ChannelHandlerContext ctx) {
                throw new RuntimeException();
            }
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse, processing);
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testRessourceFile(HttpClient.Version version, String scheme) throws IOException, InterruptedException, URISyntaxException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { i.setSslClientAuthentication(ClientAuthentication.NONE);});
        URI requestUri = new URI(scheme, null, theURL.getHost(), theURL.getPort(), "/static/index.html", null, null);
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertEquals(200, r.statusCode());
            Assertions.assertEquals("text/html", r.headers().firstValue("Content-Type").orElseThrow());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, requestUri, HttpResponse.BodyHandlers.ofString(), processResponse, new ResourceFiles());
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void test404(HttpClient.Version version, String scheme) throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(404, r.statusCode());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse);
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testMetrics(HttpClient.Version version, String scheme) throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        URI requestUri = theURL.resolve("/metric/global");
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(200, r.statusCode());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, requestUri, HttpResponse.BodyHandlers.ofString(), processResponse, new GetMetric());
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testRootRedirect(HttpClient.Version version, String scheme) throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> { });
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertEquals(version, r.version());
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(301, r.statusCode());
            Assertions.assertEquals("/static/index.html", r.headers().firstValue("location").orElseThrow());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse, new RootRedirect());
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    void testGoogle(HttpClient.Version version) throws IOException, URISyntaxException {
        URL google = new URI("https://www.google.com").toURL();
        HttpClient.Builder clientBuilder = HttpClient.newBuilder().version(version);
        clientBuilder.sslContext(getContext.apply(Collections.emptyMap()));
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(google.toURI())
                                          .GET()
                                          .build();
            Assertions.assertThrows(SSLHandshakeException.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));
        }
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testClientAuthentication(HttpClient.Version version, String scheme)
            throws IOException, InterruptedException {
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.REQUIRED));
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(200, r.statusCode());
            checkTlsPeer(r, "CN=localhost");
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse, new SimpleHandler());
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testPasswordAuthenticationFailed(HttpClient.Version version, String scheme)
            throws IOException, InterruptedException {
        AuthenticationHandler auhtHandler = AuthenticationHandler.getBuilder()
                                                    .setLogin("user").setPassword("password".toCharArray())
                                                    .build();
        resource.setAuthHandler(auhtHandler);
        resource.setVersionedModelSetup((v, p) -> {

        });
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.NONE));
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(401, r.statusCode());
        };
        runRequest(version, theURL, HttpResponse.BodyHandlers.ofString(), processResponse);
    }

    @ParameterizedTest
    @MethodSource("protocolArguments")
    @Timeout(5)
    void testPasswordAuthenticationSuccess(HttpClient.Version version, String scheme)
            throws IOException, InterruptedException {
        AuthenticationHandler auhtHandler = AuthenticationHandler.getBuilder()
                                                    .setLogin("user").setPassword("password".toCharArray())
                                                    .build();
        resource.setAuthHandler(auhtHandler);
        resource.setVersionedModelSetup((v, p) -> {

        });
        URI theURL = startHttpServer(scheme, Collections.emptyMap(), i -> i.setSslClientAuthentication(ClientAuthentication.NONE));
        Authenticator authenticator = new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(
                        "user",
                        "password".toCharArray()
                );
            }
        };
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                                                     .version(version)
                                                     .authenticator(authenticator);
        Consumer<HttpResponse<String>> processResponse = r -> {
            Assertions.assertNotNull(r.body());
            Assertions.assertEquals(404, r.statusCode());
        };
        runRequest(clientBuilder, theURL, HttpResponse.BodyHandlers.ofString(), processResponse);
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    void testClientAuthenticationFailed(HttpClient.Version version) {
        URI theURL = startHttpServer("https", Collections.emptyMap(), i -> {
            i.setSslContext(getContext.apply(Map.of("issuers", new String[] {"cn=notlocalhost"})));
            i.setSslClientAuthentication(ClientAuthentication.WANTED);
        });
        HttpClient.Builder clientBuilder = HttpClient.newBuilder().version(version);
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(theURL)
                                          .GET()
                                          .build();
            IOException ex = Assertions.assertThrows(IOException.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));
            Assertions.assertEquals("(certificate_unknown) PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target", ex.getMessage());
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    void testChangedAlias(HttpClient.Version version) {
        URI theURL = startHttpServer("https", Collections.emptyMap(), i -> i.setSslKeyAlias("invalidalias"));
        HttpClient.Builder clientBuilder = HttpClient.newBuilder().version(version);
        clientBuilder.sslContext(getContext.apply(Collections.emptyMap()));
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(theURL)
                                          .GET()
                                          .build();
            Assertions.assertThrows(IOException.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));
        }
    }

    @ParameterizedTest
    @EnumSource(HttpClient.Version.class)
    @Timeout(5)
    void testNoSsl(HttpClient.Version version) {
        // Start an SSL server but connect via http
        URI theURL = startHttpServer("http", Collections.emptyMap(), i -> { i.setWithSsl(true); i.setSslClientAuthentication(ClientAuthentication.NONE);});
        HttpClient.Builder clientBuilder = HttpClient.newBuilder().version(version);
        try (HttpClient client = clientBuilder.build()) {
            HttpRequest request = HttpRequest.newBuilder()
                                          .uri(theURL)
                                          .GET()
                                          .build();
            Assertions.assertThrows(IOException.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));
        }
    }

}
