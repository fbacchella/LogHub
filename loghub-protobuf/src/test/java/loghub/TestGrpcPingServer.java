package loghub;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelHandlerContext;
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
import loghub.protobuf.GrpcStreamHandler;
import loghub.protobuf.Ping;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;

class TestGrpcPingServer {

    private static SSLContext sslctx;
    private static SslContext nettyCtx;
    private static BinaryCodec<ChannelHandlerContext> ping;

    @BeforeAll
    static void configure() throws IOException, DescriptorValidationException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security", "loghub.HttpTestServer", "loghub.netty", "loghub.grpc", "io.netty");
        Configurator.setLevel("org", Level.WARN);
        JmxService.start(JmxService.configuration());
        sslctx = SslContextBuilder.getBuilder(Map.of("context", "TLSv1.3", "trusts", Tools.getDefaultKeyStore())).build();
        ApplicationProtocolConfig apn = new ApplicationProtocolConfig(Protocol.ALPN, SelectorFailureBehavior.FATAL_ALERT, SelectedListenerFailureBehavior.FATAL_ALERT, "h2");
        nettyCtx = new JdkSslContext(sslctx, true, null, IdentityCipherSuiteFilter.INSTANCE, apn, ClientAuth.NONE, null, false);
        ping = new Ping<>();
    }
    private final HttpTestServer resource = new HttpTestServer();

    @BeforeEach
    void register() {
        GrpcStreamHandler.Factory factory = new GrpcStreamHandler.Factory(ping);
        factory.register("ping.PingService.Ping", (o, c) -> 15L);
        resource.setModelHandlers();
        resource.setHttp2handler(ch -> ch.pipeline().addLast("GrpcStreamHandler", factory.get()));
    }

    @AfterEach
    void clean() {
        resource.after();
    }

    @BeforeEach
    void webStats() {
        Stats.reset();
        Stats.registerHttpService(resource.getHolder());
    }

    private URI startHttpServer() {
        Builder transportConfig = TcpTransport.getBuilder();
        transportConfig.setEndpoint("localhost");
        transportConfig.setWithSsl(true);
        transportConfig.setSslContext(sslctx);
        transportConfig.setSslKeyAlias("localhost (loghub ca)");
        transportConfig.setSslClientAuthentication(ClientAuthentication.NONE);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
        transportConfig.setThreadPrefix("gRPCServer");
        return resource.startServer(transportConfig);
    }

    @Test
    @Timeout(5)
    void testPing() throws InterruptedException {
        URI listenUri = startHttpServer();
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", listenUri.getPort())
                                         .useTransportSecurity()
                                         .sslContext(nettyCtx)
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

}
