package loghub;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.netty.transport.TcpTransport;
import loghub.netty.transport.TcpTransport.Builder;
import loghub.proto.ping.PingRequest;
import loghub.proto.ping.PingResponse;
import loghub.proto.ping.PingServiceGrpc;
import loghub.protobuf.BinaryCodec;
import loghub.protobuf.GrpcStatus;
import loghub.protobuf.GrpcStreamHandler;
import loghub.protobuf.Ping;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

class TestGrpcPingServer {

    private static final SSLContext sslctx = SslContextBuilder.getBuilder(Map.of("context", "TLSv1.3", "trusts", Tools.getDefaultKeyStore())).build();
    private static final SslContext nettyCtx = getNettyCtx();
    private static final HttpClient client = HttpClient.newBuilder().sslContext(sslctx).build();

    private static final HttpTestServer resource = new HttpTestServer();

    private static GrpcStreamHandler.Factory factory;
    private static URI listenUri;

    private static SslContext getNettyCtx() {
        ApplicationProtocolConfig apn = new ApplicationProtocolConfig(Protocol.ALPN, SelectorFailureBehavior.FATAL_ALERT, SelectedListenerFailureBehavior.FATAL_ALERT, "h2");
        return new JdkSslContext(sslctx, true, null, IdentityCipherSuiteFilter.INSTANCE, apn, ClientAuth.NONE, null, false);
    }

    @BeforeAll
    static void configure() throws IOException, DescriptorValidationException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer", "loghub.netty", "loghub.protobuf");
        Configurator.setLevel("org", Level.WARN);
        JmxService.start(JmxService.configuration());
        BinaryCodec<GrpcStreamHandler> ping = new Ping<>();
        factory = new GrpcStreamHandler.Factory(ping);
        factory.register("ping.PingService.Ping", (o, c) -> 15L);
        resource.setModelHandlers();
        resource.setHttp2handler(ch -> ch.pipeline().addLast("GrpcStreamHandler", factory.get()));

        Builder transportConfig = TcpTransport.getBuilder();
        transportConfig.setEndpoint("localhost");
        transportConfig.setWithSsl(true);
        transportConfig.setSslContext(sslctx);
        transportConfig.setSslKeyAlias("localhost (loghub ca)");
        transportConfig.setSslClientAuthentication(ClientAuthentication.NONE);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
        transportConfig.setThreadPrefix("gRPCServer");
        listenUri = resource.startServer(transportConfig);
        logger.info("gRPC server started on port {}", listenUri.getPort());
    }

    @AfterAll
    static void clean() {
        resource.after();
    }

    @BeforeEach
    void webStats() {
        Stats.reset();
        Stats.registerHttpService(resource.getHolder());
    }

    @Test
    @Timeout(5)
    void testPing() throws InterruptedException {
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", listenUri.getPort())
                                         .useTransportSecurity()
                                         .sslContext(nettyCtx)
                                         .build();

        PingServiceGrpc.PingServiceBlockingStub stub = PingServiceGrpc.newBlockingStub(channel);

        PingRequest request = PingRequest.newBuilder()
                                         .setMessage("ping")
                                         .build();

        PingResponse response = stub.withDeadlineAfter(5, TimeUnit.DAYS)
                                    .ping(request);

        Assertions.assertEquals("ping", response.getMessage());
        Assertions.assertEquals(15L, response.getTimestamp());
        channel.shutdown();
        channel.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(5)
    void testUnknownGrpcMethod() throws InterruptedException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
                                      .uri(listenUri.resolve("/ping.PingService/Pong"))
                                      .POST(BodyPublishers.noBody())
                                      .header("Content-Type", "application/grpc")
                                      .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(Integer.toString(GrpcStatus.UNIMPLEMENTED.getStatus()), response.headers().firstValue("grpc-status").orElse(null));
    }

    @Test
    @Timeout(5)
    void testWrongHttpMethod() throws InterruptedException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(listenUri.resolve("/ping.PingService/Ping"))
                                         .GET()
                                         .header("Content-Type", "application/grpc")
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals(Integer.toString(GrpcStatus.INVALID_ARGUMENT.getStatus()), response.headers().firstValue("grpc-status").orElse(null));
    }

    @Test
    @Timeout(5)
    void testWrongContentType() throws InterruptedException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(listenUri.resolve("/ping.PingService/Ping"))
                                         .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                                         .header("Content-Type", "text/plain")
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals(Integer.toString(GrpcStatus.INVALID_ARGUMENT.getStatus()), response.headers().firstValue("grpc-status").orElse(null));
    }

    @Test
    @Timeout(5)
    void testHttp1_1() throws InterruptedException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
                                      .uri(listenUri.resolve("/ping.PingService/Ping"))
                                      .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                                      .header("Content-Type", "text/plain")
                                      .version(Version.HTTP_1_1)
                                      .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("text/plain; charset=UTF-8", response.headers().firstValue("content-type").orElse(null));
    }

}
