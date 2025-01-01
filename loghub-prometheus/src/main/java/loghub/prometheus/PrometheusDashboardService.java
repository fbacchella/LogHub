package loghub.prometheus;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.prometheus.metrics.exporter.opentelemetry.OpenTelemetryExporter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import loghub.Helpers;
import loghub.netty.DashboardService;

/**
 * Resolved the properties as described by <a href="https://prometheus.github.io/client_java/config/config/">client_java/config</a>
 */
public class PrometheusDashboardService implements DashboardService {

    @Override
    public String getPrefix() {
        return "prometheus";
    }

    @Override
    public List<SimpleChannelInboundHandler<FullHttpRequest>> getServices(Map<String, Object> properties) {
        PrometheusRegistry registry = new PrometheusRegistry();
        PrometheusMetricsEndpoint.Builder builder = PrometheusMetricsEndpoint.getBuilder();
        if (properties.containsKey("withJvmMetrics")) {
            builder.setWithJvmMetrics(Boolean.TRUE.equals(properties.remove("withJvmMetrics")));
        }
        if (Boolean.TRUE.equals(properties.remove("withOpenTelemetry"))) {
            OpenTelemetryExporter.Builder openTelemetryExporterBuilder = OpenTelemetryExporter.builder();

            openTelemetryExporterBuilder.registry(registry);
            Map<String, Object> otlpProperties = Helpers.filterPrefix(properties, "openTelemetry");
            Optional.ofNullable(otlpProperties.remove("endpoint")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::endpoint);
            Optional.ofNullable(otlpProperties.remove("protocol")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::protocol);
            Optional.ofNullable(otlpProperties.remove("interval")).map(Integer.class::cast).ifPresent(openTelemetryExporterBuilder::intervalSeconds);
            Optional.ofNullable(otlpProperties.remove("timeoutSeconds")).map(Integer.class::cast).ifPresent(openTelemetryExporterBuilder::timeoutSeconds);
            Optional.ofNullable(otlpProperties.remove("headers")).map(Map.class::cast).ifPresent(m -> fillMap(m, openTelemetryExporterBuilder::header));
            Optional.ofNullable(otlpProperties.remove("resourceAttributes")).map(Map.class::cast).ifPresent(m -> fillMap(m, openTelemetryExporterBuilder::resourceAttribute));
            Optional.ofNullable(otlpProperties.remove("serviceInstanceId")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::serviceInstanceId);
            Optional.ofNullable(otlpProperties.remove("serviceNamespace")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::serviceNamespace);
            Optional.ofNullable(otlpProperties.remove("serviceName")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::serviceName);
            Optional.ofNullable(otlpProperties.remove("serviceVersion")).map(String.class::cast).ifPresent(openTelemetryExporterBuilder::serviceVersion);
            openTelemetryExporterBuilder.buildAndStart();
        }
        builder.setRegistry(registry);
        PrometheusMetricsEndpoint ps = builder.build();
        return List.of(ps);
    }

    private void fillMap(Map<?, ?> m, BiConsumer<String, String> filler) {
        m.forEach((k, v) -> filler.accept(k.toString(), v.toString()));
    }

}
