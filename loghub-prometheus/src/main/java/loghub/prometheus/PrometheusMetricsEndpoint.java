package loghub.prometheus;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.config.PrometheusPropertiesLoader;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.exporter.common.PrometheusScrapeHandler;
import io.prometheus.metrics.expositionformats.ExpositionFormatWriter;
import io.prometheus.metrics.expositionformats.ExpositionFormats;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoCache
@RequestAccept(path = "/prometheus")
class PrometheusMetricsEndpoint extends HttpRequestProcessing {

    private static final Map<String, Unit> units = new ConcurrentHashMap<>();
    private static final AttributeKey<NettyPrometheusHttpExchange> EXCHANGE_KEY = AttributeKey.newInstance(NettyPrometheusHttpExchange.class.getCanonicalName());

    @Setter
    @Accessors(chain = true)
    public static class Builder {
        private boolean withJvmMetrics = true;
        private PrometheusRegistry registry = new PrometheusRegistry();
        private PrometheusProperties prometheusProperties = PrometheusPropertiesLoader.load();
        PrometheusMetricsEndpoint build() {
            return new PrometheusMetricsEndpoint(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final PrometheusScrapeHandler prometheusScrapeHandler;
    private final ExpositionFormats expositionFormats;

    public PrometheusMetricsEndpoint(Builder builder) {
        if (builder.withJvmMetrics) {
            JvmMetrics.builder().register(builder.registry);
        }
        enumerateBeans(builder.prometheusProperties, builder.registry);

        prometheusScrapeHandler = new PrometheusScrapeHandler(builder.prometheusProperties, builder.registry);
        expositionFormats = ExpositionFormats.init(builder.prometheusProperties.getExporterProperties());
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/prometheus");
    }

    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        NettyPrometheusHttpExchange exchange = new NettyPrometheusHttpExchange(this, request, ctx);
        ctx.channel().attr(EXCHANGE_KEY).set(exchange);
        try {
            prometheusScrapeHandler.handleRequest(exchange);
            if (exchange.status == 200) {
                ByteBuf content = Unpooled.wrappedBuffer(exchange.content.toByteArray());
                writeResponse(ctx, request, content, content.readableBytes());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Failed request processing");
        }
    }

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        String acceptHeader = request.headers().get("Accept");
        ExpositionFormatWriter writer = expositionFormats.findWriter(acceptHeader);
        return writer.getContentType();
    }

    /**
     * Can be used to add custom handlers to a response, call by {@link #writeResponse(ChannelHandlerContext, FullHttpRequest, ByteBuf, int)}.
     * So if processRequest don't call it, no handlers will be added
     *
     * @param request
     * @param response
     * @param ctx
     */
    @Override
    protected void addCustomHeaders(HttpRequest request, HttpResponse response, ChannelHandlerContext ctx) {
        NettyPrometheusHttpExchange exchange = ctx.channel().attr(EXCHANGE_KEY).get();
        exchange.copyHeader(response.headers());
    }

    private void enumerateBeans(PrometheusProperties config, PrometheusRegistry registry) {
        try {
            MBeanServerConnection beanConn = ManagementFactory.getPlatformMBeanServer();
            ObjectName baseMbeans = new ObjectName("loghub", new Hashtable<>(Map.of("type", "*")));
            for (ObjectName mBeanName : beanConn.queryNames(baseMbeans, null)) {
                // Don't export exceptions
                if ("Exceptions".equals(mBeanName.getKeyProperty("type"))) {
                    continue;
                }
                try {
                    MBeanInfo mBeanInfo = beanConn.getMBeanInfo(mBeanName);
                    for (MBeanAttributeInfo mBeanAttributeInfo : mBeanInfo.getAttributes()) {
                        // Only resolve metrics attribute
                        if (mBeanAttributeInfo.isReadable() && !mBeanAttributeInfo.isWritable()) {
                            resolveAttribute(beanConn, mBeanName, mBeanAttributeInfo, config, registry);
                        }
                    }
                } catch (InstanceNotFoundException | IntrospectionException | ReflectionException | IOException e) {
                    // Skip this
                }
            }
        } catch (MalformedObjectNameException | IOException e) {
            throw new IllegalStateException("Unusable JMX state", e);
        }

    }

    private void resolveAttribute(MBeanServerConnection beanConn, ObjectName mBeanName, MBeanAttributeInfo attributeInfo, PrometheusProperties config, PrometheusRegistry registry) {
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
            CounterWithCallback.builder(config)
                    .name(metricName)
                    .help(attributeInfo.getDescription())
                    .unit(unit)
                    .callback(callback -> callback.call(doCallback(beanConn, mBeanName, attributeName)))
                    .register(registry);
        } else if ("gauge".equals(metric)) {
            GaugeWithCallback.builder(config)
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
        } catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException | IOException e) {
            throw new IllegalStateException("Unusable JMX state", e);
         }
    }

}
