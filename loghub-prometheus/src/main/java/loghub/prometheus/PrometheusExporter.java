package loghub.prometheus;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.exporter.common.PrometheusScrapeHandler;
import io.prometheus.metrics.expositionformats.ExpositionFormatWriter;
import io.prometheus.metrics.expositionformats.ExpositionFormats;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;

@NoCache
@RequestAccept(path = "/prometheus")
class PrometheusExporter extends HttpRequestProcessing {

    private static final AttributeKey<NettyPrometheusHttpExchange> EXCHANGE_KEY = AttributeKey.newInstance(NettyPrometheusHttpExchange.class.getCanonicalName());

    private final PrometheusScrapeHandler prometheusScrapeHandler;
    private final ExpositionFormats expositionFormats;

    public PrometheusExporter(PrometheusRegistry registry, PrometheusProperties prometheusProperties) {
        prometheusScrapeHandler = new PrometheusScrapeHandler(prometheusProperties, registry);
        expositionFormats = ExpositionFormats.init(prometheusProperties.getExporterProperties());
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

}
