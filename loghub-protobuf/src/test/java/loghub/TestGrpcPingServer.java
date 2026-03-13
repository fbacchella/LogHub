package loghub;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import loghub.proto.ping.PingRequest;
import loghub.proto.ping.PingResponse;
import loghub.proto.ping.PingServiceGrpc;
import loghub.protobuf.BinaryCodec;
import loghub.protobuf.GrpcStatus;
import loghub.protobuf.GrpcStreamHandler;
import loghub.protobuf.Ping;

class TestGrpcPingServer {

    @TempDir
    static Path tempDir;

    private static TlsContext tlsContext;
    private static ServerContext serverContext;
    private static HttpClient client;

    @BeforeAll
    static void configure() throws IOException, DescriptorValidationException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "loghub.protobuf");
        tlsContext = new TlsContext(tempDir);
        client = HttpClient.newBuilder().sslContext(tlsContext.sslctx).build();
        BinaryCodec<GrpcStreamHandler> ping = new Ping<>();
        GrpcStreamHandler.Factory factory = new GrpcStreamHandler.Factory(ping);
        factory.register("ping.PingService.Ping", (o, c) -> 15L);
        serverContext = new ServerContext(tlsContext, factory);
        logger.info("gRPC server started on port {}", serverContext.listenUri.getPort());
    }

    @AfterAll
    static void clean() {
        serverContext.clean();
    }

    @BeforeEach
    void webStats() {
        serverContext.resetStats();
    }

    @Test
    //@Timeout(5)
    void testPing() throws InterruptedException {
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", serverContext.listenUri.getPort())
                                         .useTransportSecurity()
                                         .sslContext(tlsContext.nettyCtx)
                                         .build();

        PingServiceGrpc.PingServiceBlockingStub stub = PingServiceGrpc.newBlockingStub(channel);

        PingRequest request = PingRequest.newBuilder()
                                         .setMessage("ping")
                                         .build();

        PingResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
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
                                      .uri(serverContext.listenUri.resolve("/ping.PingService/Pong"))
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
                                         .uri(serverContext.listenUri.resolve("/ping.PingService/Ping"))
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
                                         .uri(serverContext.listenUri.resolve("/ping.PingService/Ping"))
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
                                      .uri(serverContext.listenUri.resolve("/ping.PingService/Ping"))
                                      .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                                      .header("Content-Type", "text/plain")
                                      .version(Version.HTTP_1_1)
                                      .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("text/plain; charset=UTF-8", response.headers().firstValue("content-type").orElse(null));
    }

}
