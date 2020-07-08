package loghub.netty.http;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import loghub.Helpers;
import lombok.Data;

@ContentType("application/json; charset=utf-8")
public class GetMetric extends HttpRequestProcessing implements ChannelHandler {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    private static final Pattern partsExtractor = Pattern.compile("/metric/(?<metricname>(?<type>[a-z]+?)(s?|(/(?<name>.*))))");

    @Data
    private static class MetricEntry {
        private final String description;
        private final String addendum;
        private final String url;
        private final String format;
        private final String snag;
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/metric/");
    }

    @Override
    protected void processRequest(FullHttpRequest request,
                                  ChannelHandlerContext ctx)
                                                  throws HttpRequestFailure {
        try {
            Matcher m = partsExtractor.matcher(URLDecoder.decode(request.uri(), "UTF-8"));
            if (m.matches()) {
                MetricEntry[] metrics = null;
                switch (m.group("type")) {
                case "global":
                    metrics = getGlobalMetrics();
                    break;
                case "receiver":
                    metrics = getReceiverMetrics(m.group("name"));
                    break;
                case "pipeline":
                    metrics = getSenderMetrics(m.group("name"));
                    break;
                case "sender":
                    metrics = getPipelineMetrics(m.group("name"));
                    break;
                default:
                    throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, String.format("Unsupported metric name: %s", m.group("metricname")));
                }
                String serialized = json.get().writeValueAsString(metrics);
                ByteBuf content = Unpooled.copiedBuffer(serialized + "\r\n", CharsetUtil.UTF_8);
                writeResponse(ctx, request, content, content.readableBytes());
            } else {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, String.format("Unsupported metric name: %s", request.uri().replace("/metric/", "")));
            }
        } catch (UnsupportedEncodingException ex) {
            throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, String.format("malformed metric class %s: %s", request.uri(), ex.getMessage()));
        } catch (JsonProcessingException e) {
            logger.error("Unable to handle json response", e);
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format("Unable to handle json response: %s", Helpers.resolveThrowableException(e)));
        }
    }

    private MetricEntry[] getGlobalMetrics() {
        return new MetricEntry[] {
           new MetricEntry("JVM Heap", "bytes", "/jmx/java.lang:type=Memory", ",.3s", "return res[\"HeapMemoryUsage\"][\"used\"]"),
           new MetricEntry("Event received", "e/s", "/jmx/loghub:type=Receivers,level=details,name=count", ",.2s", "return res[\"OneMinuteRate\"]"),
           new MetricEntry("In flight", "Event being processed", "/jmx/loghub:type=Global,level=details,name=inflight", ",", "return res[\"Count\"]"),
           new MetricEntry("95% latency", "s", "/jmx/loghub:type=Global,level=details,name=timer", ",.2s", "return res[\"95thPercentile\"] / 1000"),
           new MetricEntry("Event waiting", "In the processing loop", "/jmx/loghub:type=Global,level=details,name=waitingProcessing", ",", "return res[\"Value\"]"),
        };
    }

    private MetricEntry[] getReceiverMetrics(String name) {
        String pathBase;
        if (name == null) {
            pathBase = "/jmx/loghub:type=Receivers,level=details,name=";
        } else {
            pathBase = String.format("/jmx/loghub:type=Receivers,servicename=%s,name=", name);
        }
        return new MetricEntry[] {
           new MetricEntry("Blocked", "e/s", pathBase + "blocked", ",.2s", "return res[\"OneMinuteRate\"]"),
           new MetricEntry("Bytes", "bytes", pathBase + "bytes", ",.2s", "return res[\"OneMinuteRate\"]"),
           new MetricEntry("Event received", "e/s", pathBase + "count", ",.2s", "return res[\"OneMinuteRate\"]"),
        };
    }
    
    private MetricEntry[] getSenderMetrics(String name) {
        String pathBase;
        if (name == null) {
            pathBase = "/jmx/loghub:type=Senders,level=details,name=";
        } else {
            pathBase = String.format("/jmx/loghub:type=Senders,servicename=%s,name=", name);
        }
        return new MetricEntry[] {
           new MetricEntry("Failed", "e/s", pathBase + "failedSend", ",.2s", "return res[\"OneMinuteRate\"]"),
           new MetricEntry("Sent", "e/s", pathBase + "sent", ",.2s", "return res[\"OneMinuteRate\"]"),
        };
    }

    private MetricEntry[] getPipelineMetrics(String name) {
        String pathBase;
        if (name == null) {
            pathBase = "/jmx/loghub:type=Pipelines,level=details,name=";
        } else {
            pathBase = String.format("/jmx/loghub:type=Pipelines,servicename=%s,name=", name);
        }
        return new MetricEntry[] {
           new MetricEntry("Failed", "e/s", pathBase + "failed", ",.2s", "return res[\"OneMinuteRate\"]"),
           new MetricEntry("Dropped", "e/s", pathBase + "dropped", ",.2s", "return res[\"OneMinuteRate\"]"),
        };
    }
}
