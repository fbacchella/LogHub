package loghub.receivers;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.centreon.agent.Agent.AgentInfo;
import com.centreon.agent.Agent.MessageFromAgent;
import com.centreon.agent.Agent.MessageToAgent;
import com.centreon.agent.Agent.Version;
import com.centreon.agent.AgentServiceGrpc;
import com.centreon.agent.AgentServiceGrpc.AgentServiceStub;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;
import loghub.LogUtils;
import loghub.TlsContext;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;

class TestCentreonAgent {

    @TempDir
    static Path tempDir;

    private static TlsContext tlsContext;
    private static Logger logger;
    private int port;

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "loghub.grpc", "loghub.receivers", "io.grpc", "io.netty");
        tlsContext = new TlsContext(tempDir);
    }

    private void doRequest() throws InterruptedException {
        AgentInfo info = AgentInfo.newBuilder()
                                 .setHost("loghubserver")
                                 .setCentreonVersion(Version.newBuilder()
                                                             .setMajor(24).setMinor(10).setPatch(7)
                                                             .build())
                                 .build();

        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", port)
                                         .useTransportSecurity()
                                         .sslContext(tlsContext.nettyCtx)
                                         .build();
        AgentServiceStub stub = AgentServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<MessageFromAgent> requestObserver = stub.withDeadlineAfter(5, TimeUnit.DAYS)
                                                               .export(new StreamObserver<>() {
                                                                   @Override
                                                                   public void onNext(MessageToAgent value) {
                                                                       logger.debug("Received message from server: {}",
                                                                               value);
                                                                   }

                                                                   @Override
                                                                   public void onError(Throwable t) {
                                                                       logger.error("Error from server: {}", t.getMessage());
                                                                       latch.countDown();
                                                                   }

                                                                   @Override
                                                                   public void onCompleted() {
                                                                       logger.debug("Server completed the stream");
                                                                       latch.countDown();
                                                                   }
                                                               });

        requestObserver.onNext(MessageFromAgent.newBuilder().setInit(info).build());

        long tsNanos = Instant.now().toEpochMilli() * 1_000_000L;
        Metric jvmHeapUsed = Metric.newBuilder()
                                   .setName("jvm.memory.used")
                                   .setSum(Sum.newBuilder()
                                              .setIsMonotonic(false)
                                              .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                                              .addDataPoints(NumberDataPoint.newBuilder()
                                                                            .setTimeUnixNano(tsNanos)
                                                                            .setAsInt(512 * 1024 * 1024L)
                                                                            .addAttributes(KeyValue.newBuilder()
                                                                                                   .setKey("jvm.memory.pool.name")
                                                                                                   .setValue(AnyValue.newBuilder().setStringValue("G1 Eden Space").build())
                                                                                                   .build())
                                                                            .build())
                                              .build())
                                   .build();

        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                                                .setScope(InstrumentationScope.newBuilder()
                                                                              .setName("com.example.mon-service.metrics")
                                                                              .setVersion("1.0.0")
                                                                              .build())
                                                .addMetrics(jvmHeapUsed)
                                                .build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                                                         .setResource(Resource.newBuilder()
                                                                              .addAttributes(KeyValue.newBuilder()
                                                                                                     .setKey("service.name")
                                                                                                     .setValue(AnyValue.newBuilder().setStringValue("mon-service").build())
                                                                                                     .build())
                                                                              .build())
                                                         .addScopeMetrics(scopeMetrics)
                                                         .build();

        ExportMetricsServiceRequest otelRequest = ExportMetricsServiceRequest.newBuilder()
                                                                             .addResourceMetrics(resourceMetrics)
                                                                             .build();

        requestObserver.onNext(MessageFromAgent.newBuilder().setOtelRequest(otelRequest).build());
        requestObserver.onCompleted();

        latch.await(5, TimeUnit.DAYS);
        channel.shutdown().awaitTermination(5, TimeUnit.DAYS);
    }

    @Test
    //@Timeout(5)
    void runGrpc() throws IOException, InterruptedException {
        port = Tools.tryGetPort();
        String confile = """
                input {
                    loghub.receivers.GrpcReceiver {
                        port: %1$d,
                        grpcCodecs: [
                            loghub.decoders.CentreonAgent,
                            loghub.decoders.OpenTelemetry,
                        ],
                    }
                } | $main
                pipeline[main] {}
                ssl.trusts: ["%2$s"]
                """.formatted(port, tempDir.resolve("loghub.p12"));
        Properties conf = Tools.loadConf(new StringReader(confile));
        try (Receiver<?, ?> r = conf.receivers.stream().findAny().orElseThrow()) {
            r.start();
            doRequest();
            Event ev = conf.mainQueue.poll(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(ev, "Event was not received");
            Assertions.assertEquals("com.centreon.agent.AgentService.Export", ev.getMeta("gRPCMethod"));
            Assertions.assertEquals("/com.centreon.agent.AgentService/Export", ev.getMeta("url_path"));
            Assertions.assertTrue(ev.getMeta("user_agent").toString().startsWith("grpc-java-netty/"));
            Assertions.assertTrue(ev.getMeta("host_header").toString().startsWith("localhost:"));
            List<Map<String, Object>> resourceMetrics = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("resource_metrics"));
            Assertions.assertNotNull(resourceMetrics, "resource_metrics should not be null");
            Map<String, Object> firstRM = resourceMetrics.getFirst();
            Assertions.assertEquals(3, firstRM.size());
            List<Map<String, Object>> scopeMetrics = (List<Map<String, Object>>) firstRM.get("scope_metrics");
            Assertions.assertNotNull(scopeMetrics, "scope_metrics should not be null");
            Assertions.assertEquals(1, scopeMetrics.size());
            Map<String, Object> firstSM = scopeMetrics.getFirst();
            List<Map<String, Object>> metrics = (List<Map<String, Object>>) firstSM.get("metrics");
            Assertions.assertNotNull(metrics, "metrics should not be null");
            Map<String, Object> firstM = metrics.getFirst();
            Assertions.assertEquals("jvm.memory.used", firstM.get("name"));
        }
    }

}
