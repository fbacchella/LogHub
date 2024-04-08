package loghub.httpclient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.AbstractBuilder;
import loghub.LogUtils;
import loghub.Tools;
import loghub.jackson.JacksonBuilder;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.transport.POLLER;
import loghub.netty.transport.TRANSPORT;
import loghub.netty.transport.TcpTransport;

public class TestApacheHttpClient {

    private static final ObjectWriter writer = JacksonBuilder.get(JsonMapper.class)
                                                             .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                                             .getWriter();

    @ContentType("application/json; charset=utf-8")
    private static class Echo extends HttpRequestProcessing implements ChannelInboundHandler {

        @Override
        protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
            try {
                Map.Entry<String, Object> e1 = Map.entry("URI", request.uri());
                Map.Entry<String, Object> e2 = Map.entry("VERB", request.method().name());
                Map.Entry<String, Object> e3 = Map.entry("HEADERS", request.headers()
                                                              .entries()
                                                              .stream()
                                                              .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                        Map.Entry::getValue,
                                                                                        (v1, v2) -> v1 + ", " + v2))
                );
                Map<String, Object> values = Map.ofEntries(e1, e2, e3);
                ByteBuf content = ctx.alloc().buffer();
                content.writeCharSequence(writer.writeValueAsString(values), StandardCharsets.UTF_8);
                writeResponse(ctx, request, content, content.readableBytes());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        @Override
        public boolean acceptRequest(io.netty.handler.codec.http.HttpRequest request) {
            return true;
        }
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.httpclient", "org.apache.hc", "io.netty");
    }

    static TcpTransport transport;
    static int port;

    @Before
    public void startServer() throws InterruptedException {
        HttpChannelConsumer consumer = HttpChannelConsumer.getBuilder()
                                               .setModelSetup(p -> p.addLast(new Echo()))
                                               .build();

        TcpTransport.Builder transportBuilder = TRANSPORT.TCP.getBuilder();
        transportBuilder.setThreadPrefix("Dashboard");
        transportBuilder.setPoller(POLLER.DEFAULTPOLLER);
        transportBuilder.setConsumer(consumer);
        transportBuilder.setEndpoint("127.0.0.1");
        transportBuilder.setPort(0);

        transport = transportBuilder.build();
        transport.bind();
        transport.getChannels()
                 .map(Channel::localAddress)
                 .map(InetSocketAddress.class::cast)
                 .mapToInt(InetSocketAddress::getPort)
                 .findAny()
                .ifPresent(p -> port = p);
    }

    @AfterClass
    public static void stopServer() {
        Optional.ofNullable(transport).ifPresent(TcpTransport::close);
    }

    @Test
    public void testFailed() throws IOException, URISyntaxException {
        transport.close();
        ApacheHttpClientService.Builder clientBuilder = ApacheHttpClientService.getBuilder();
        clientBuilder.setTimeout(10);
        clientBuilder.setUser("bla");
        clientBuilder.setPassword("blo");
        clientBuilder.setWorkers(2);
        clientBuilder.setSslKeyAlias(null);

        AbstractHttpClientService httpClient = clientBuilder.build();
        HttpRequest<Map<String, Object>> request = httpClient.getRequest();
        request.addHeader("X-Test-Header", "true");
        request.setHttpVersion(2,0);
        request.setUri(new URI("http", null, "localhost", port, "/", "q=true", "#fragment"));
        request.setVerb("PUT");
        try (HttpResponse<Map<String, Object>> response = httpClient.doRequest(request)) {
            Assert.assertTrue(response.isConnexionFailed());
        }
    }

    @Test
    public void testConnection() throws IOException, URISyntaxException {
        ApacheHttpClientService.Builder clientBuilder = ApacheHttpClientService.getBuilder();
        clientBuilder.setTimeout(10);
        clientBuilder.setUser("bla");
        clientBuilder.setPassword("blo");
        clientBuilder.setWorkers(2);
        clientBuilder.setSslKeyAlias(null);

        AbstractHttpClientService httpClient = clientBuilder.build();
        HttpRequest<Map<String, Object>> request = httpClient.getRequest();
        request.addHeader("X-Test-Header", "true");
        request.setHttpVersion(2,0);
        request.setUri(new URI("http", null, "localhost", port, "/", "q=true", "#fragment"));
        request.setVerb("PUT");
        ObjectReader reader = JacksonBuilder.get(JsonMapper.class)
                                      .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                      .getReader();
        request.setConsumeText(reader::readValue);
        try (HttpResponse<Map<String, Object>> response = httpClient.doRequest(request)) {
            Assert.assertEquals(200, response.getStatus());
            Assert.assertEquals("application/json", response.getMimeType().getMimeType());
            Map<String, Object> data = response.getParsedResponse();
            Assert.assertEquals("/?q=true", data.get("URI"));
            Assert.assertEquals("PUT", data.get("VERB"));
            @SuppressWarnings("unchecked")
            Map<String, Object> headers = (Map<String, Object>) data.get("HEADERS");
            Assert.assertEquals("true", headers.get("X-Test-Header"));
            Assert.assertEquals("true", headers.get("X-Test-Header"));
        }
    }

    @Test
    public void testLoad() throws InvocationTargetException, ClassNotFoundException {
        @SuppressWarnings("unchecked")
        Class<AbstractHttpClientService> clientClass = (Class<AbstractHttpClientService>) this.getClass().getClassLoader().loadClass("loghub.httpclient.ApacheHttpClientService");
        @SuppressWarnings("unchecked")
        AbstractHttpClientService.Builder<ApacheHttpClientService> builder = (AbstractHttpClientService.Builder) AbstractBuilder.resolve(clientClass);
        Assert.assertEquals(ApacheHttpClientService.Builder.class, builder.getClass());
    }

}
