package loghub.prometheus;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.config.PrometheusPropertiesLoader;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.exporter.opentelemetry.OpenTelemetryExporter;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;
import loghub.Helpers;
import loghub.netty.DashboardService;

/**
 * Resolved the properties as described by <a href="https://prometheus.github.io/client_java/config/config/">client_java/config</a>
 */
public class PrometheusDashboardService implements DashboardService {

    private static final Logger logger = LogManager.getLogger();

    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final Map<String, Unit> units = new HashMap<>();
    private final PrometheusProperties prometheusProperties = PrometheusPropertiesLoader.load();

    @Override
    public String getName() {
        return "prometheus";
    }

    @Override
    public List<SimpleChannelInboundHandler<FullHttpRequest>> getHandlers(Map<String, Object> properties) {
        if (Boolean.TRUE.equals(properties.remove("withJvmMetrics"))) {
            JvmMetrics.builder().register(registry);
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
        if (Boolean.TRUE.equals(properties.remove("withExporter"))) {
            PrometheusExporter ps = new PrometheusExporter(registry, prometheusProperties);
            return List.of(ps);
        } else {
            return List.of();
        }
    }

    private void fillMap(Map<?, ?> m, BiConsumer<String, String> filler) {
        m.forEach((k, v) -> filler.accept(k.toString(), v.toString()));
    }

    @Override
    public List<Runnable> getStarters() {
        return List.of(this::enumerateBeans);
    }

    private void enumerateBeans() {
        try {
            MBeanServer beanConn = ManagementFactory.getPlatformMBeanServer();
            ObjectName baseMbeans = new ObjectName("loghub", new Hashtable<>(Map.of("type", "*")));
            for (ObjectName mBeanName : beanConn.queryNames(baseMbeans, null)) {
                // Don't export exceptions
                if ("Exceptions".equals(mBeanName.getKeyProperty("type"))) {
                    continue;
                }
                processByName(beanConn, mBeanName);
            }
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Unusable JMX state", e);
        } finally {
            units.clear();
        }
    }

    private void processByName(MBeanServer beanConn, ObjectName mBeanName) {
        try {
            MBeanInfo mBeanInfo = beanConn.getMBeanInfo(mBeanName);
            for (MBeanAttributeInfo mBeanAttributeInfo : mBeanInfo.getAttributes()) {
                // Only resolve metrics attribute
                if (mBeanAttributeInfo.isReadable() && !mBeanAttributeInfo.isWritable()) {
                    resolveAttribute(beanConn, mBeanName, mBeanAttributeInfo);
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException e) {
            // Skip this
        }
    }

    private void resolveAttribute(MBeanServerConnection beanConn, ObjectName mBeanName, MBeanAttributeInfo attributeInfo) {
        String attributeName = attributeInfo.getName();
        String metricName = String.format("loghub_%s_%s", mBeanName.getKeyProperty("type").toLowerCase(Locale.US), attributeName.toLowerCase(Locale.US));
        Unit unit = Optional.ofNullable(attributeInfo.getDescriptor().getFieldValue("units"))
                            .map(Object::toString)
                            .map(u -> units.computeIfAbsent(u, Unit::new))
                            .orElse(null);
        String metric = Optional.ofNullable(attributeInfo.getDescriptor().getFieldValue("metricType"))
                                .map(Object::toString)
                                .orElse(null);
        if (unit == null) {
            logger.info("Missing unit for {}", metricName);
        } else if (metric == null) {
            logger.info("Missing metric for {}", metricName);
        } else if ("counter".equals(metric)) {
            CounterWithCallback.builder(prometheusProperties)
                    .name(metricName)
                    .help(attributeInfo.getDescription())
                    .unit(unit)
                    .callback(callback -> callback.call(doCallback(beanConn, mBeanName, attributeName)))
                    .register(registry);
        } else if ("gauge".equals(metric)) {
            GaugeWithCallback.builder(prometheusProperties)
                    .name(metricName)
                    .help(attributeInfo.getDescription())
                    .unit(unit)
                    .callback(callback -> callback.call(doCallback(beanConn, mBeanName, attributeInfo.getName())))
                    .register(registry);
        }
    }

    private double doCallback(MBeanServerConnection beanConn, ObjectName mBeanName, String attributeName) {
        try {
            Object value = beanConn.getAttribute(mBeanName, attributeName);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else {
                return Double.NaN;
            }
        } catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException |
                 IOException e) {
            throw new IllegalStateException("Unusable JMX state", e);
        }
    }

}
