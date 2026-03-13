package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc.MetricsServiceBlockingStub;
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
import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.TlsContext;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.decoders.CodecProvider;
import loghub.events.Event;
import loghub.security.ssl.ClientAuthentication;

class TestOpenTelemetry {

    @TempDir
    static Path tempDir;

    private static TlsContext tlsContext;
    private static Logger logger;

    private GrpcReceiver receiver = null;
    private int port;

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "loghub.protobuf", "loghub.receivers");
        tlsContext = new TlsContext(tempDir);
    }

    @AfterEach
    void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private void doRequest() {
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", port)
                                         .useTransportSecurity()
                                         .sslContext(tlsContext.nettyCtx)
                                         .build();
        MetricsServiceBlockingStub stub = MetricsServiceGrpc.newBlockingStub(channel);

        long tsNanos = Instant.now().toEpochMilli() * 1_000_000L;

        Metric jvmHeapUsed = Metric.newBuilder()
                                     .setName("jvm.memory.used")
                                     .setDescription("Used JVM heap")
                                     .setUnit("By")
                                     .setSum(Sum.newBuilder()
                                                     .setIsMonotonic(false)
                                                     .setAggregationTemporality(
                                                             AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                                                     .addDataPoints(NumberDataPoint.newBuilder()
                                                                            .setTimeUnixNano(tsNanos)
                                                                            .setAsInt(512 * 1024 * 1024L) // 512 Mio
                                                                            .addAttributes(KeyValue.newBuilder()
                                                                                                   .setKey("jvm.memory.pool.name")
                                                                                                   .setValue(AnyValue.newBuilder().setStringValue("G1 Eden Space").build())
                                                                                                   .build())
                                                                            .build())
                                                     .build())
                                     .build();

        InstrumentationScope scope = InstrumentationScope.newBuilder()
                                             .setName("com.example.mon-service.metrics")
                                             .setVersion("1.0.0")
                                             .build();

        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                                            .setScope(scope)
                                            .addMetrics(jvmHeapUsed)
                                            .build();

        Resource resource = Resource.newBuilder()
                                    .addAttributes(KeyValue.newBuilder()
                                                           .setKey("service.name")
                                                           .setValue(AnyValue.newBuilder().setStringValue("mon-service").build())
                                                           .build())
                                    .addAttributes(KeyValue.newBuilder()
                                                           .setKey("service.version")
                                                           .setValue(AnyValue.newBuilder().setStringValue("1.4.2").build())
                                                           .build())
                                    .addAttributes(KeyValue.newBuilder()
                                                           .setKey("host.name")
                                                           .setValue(AnyValue.newBuilder().setStringValue("prod-host-01").build())
                                                           .build())
                                    .addAttributes(KeyValue.newBuilder()
                                                           .setKey("deployment.environment")
                                                           .setValue(AnyValue.newBuilder().setStringValue("production").build())
                                                           .build())
                                    .build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                                                  .setResource(resource)
                                                  .addScopeMetrics(scopeMetrics)
                                                  .build();
        ExportMetricsServiceRequest m = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
        ExportMetricsServiceResponse  response = stub.withDeadlineAfter(5, TimeUnit.SECONDS).export(m);
        Assertions.assertEquals(0L, response.getPartialSuccess().getRejectedDataPoints());
        Assertions.assertEquals("", response.getPartialSuccess().getErrorMessage());
    }

    @Test
    @Timeout(5)
    void runGrpc() throws IOException, InterruptedException {
        port = Tools.tryGetPort();
        String confile = """
                input {
                    loghub.receivers.GrpcReceiver {
                        port: %1$d,
                        grpcCodecs: [
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
            Assertions.assertEquals("opentelemetry.proto.collector.metrics.v1.MetricsService.Export", ev.getMeta("gRPCMethod"));
            Assertions.assertEquals("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", ev.getMeta("url_path"));
            Assertions.assertTrue(ev.getMeta("user_agent").toString().startsWith("grpc-java-netty/"));
            Assertions.assertTrue(ev.getMeta("host_header").toString().startsWith("localhost:"));
            List<Map<String, Object>> resourceMetrics = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("resource_metrics"));
            Map<String, Object> firstRM = resourceMetrics.getFirst();
            Assertions.assertEquals(3, firstRM.size());
            List<Map<String, Object>> scopeMetrics = (List<Map<String, Object>>) firstRM.get("scope_metrics");
            Assertions.assertEquals(1, scopeMetrics.size());
            Map<String, Object> firstSM = scopeMetrics.getFirst();
            List<Map<String, Object>> metrics = (List<Map<String, Object>>) firstSM.get("metrics");
            Map<String, Object> firstM = metrics.getFirst();
            Assertions.assertEquals("jvm.memory.used", firstM.get("name"));
        }
    }

    @Test
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.GrpcReceiver"
                , BeanChecks.BeanInfo.build("grpcCodecs", CodecProvider[].class)
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
                , BeanInfo.build("workerThreads", Integer.TYPE)
        );
    }

}
