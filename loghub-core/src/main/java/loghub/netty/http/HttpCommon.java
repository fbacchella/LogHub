package loghub.netty.http;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.datetime.DatetimeProcessor;
import loghub.metrics.Stats;
import loghub.netty.HttpChannelConsumer;

public interface HttpCommon<R, H> extends ChannelInboundHandler {

    String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    DatetimeProcessor DATE_FORMATTER = DatetimeProcessor.of("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                                                         .withLocale(Locale.US)
                                                         .withDefaultZone(ZoneOffset.UTC);

    default void addNoCacheHeaders(H responseHeaders) {
        setResponseHeader(responseHeaders, HttpHeaderNames.CACHE_CONTROL, "private, max-age=0");
        setResponseHeader(responseHeaders, HttpHeaderNames.EXPIRES, "-1");
    }

    default void addLogger(ChannelFuture sendFuture, String method, String uri, int status, String message) {
        sendFuture.addListener(
                (ChannelFutureListener) future -> getLogger().info("{} {}: {} {}", method, uri, status, message));
    }

    default void addCustomHeaders(R request, H responseHeaders, ChannelHandlerContext ctx) {
        addCustomHeadersCommon(request, responseHeaders);
    }

    default void addCustomHeadersCommon(R request, H responseHeaders) {
    }

    default Instant getContentDateInstant(R request, H responseHeaders) {
        return Instant.now();
    }

    default void addContentDate(R request, H responseHeaders, ChannelHandlerContext ctx) {
        addContentDateCommon(request, responseHeaders);
    }

    default void addContentDateCommon(R request, H responseHeaders) {
        Instant contentDate = getContentDateInstant(request, responseHeaders);
        if (contentDate != null) {
            setResponseHeader(responseHeaders, HttpHeaderNames.LAST_MODIFIED, DATE_FORMATTER.print(contentDate));
        }
    }

    default String getContentType(R request, H responseHeaders, ChannelHandlerContext ctx) {
        return getContentTypeCommon(request, responseHeaders);
    }

    default String getContentTypeCommon(R request, H responseHeaders) {
        ContentType ct = getClass().getAnnotation(ContentType.class);
        if (ct != null) {
            return ct.value();
        } else {
            return null;
        }
    }

    default void addContentType(R request, H responseHeaders, ChannelHandlerContext ctx) {
        String contentType = getContentType(request, responseHeaders, ctx);
        if (contentType != null) {
            setResponseHeader(responseHeaders, HttpHeaderNames.CONTENT_TYPE, contentType);
        }
    }

    default void doStatusMetric(ChannelHandlerContext ctx, HttpResponseStatus status) {
        Object holder = ctx.channel().attr(HttpChannelConsumer.HOLDERATTRIBUTE).get();
        Long startTime = ctx.channel().attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
        if (holder != null && startTime != null) {
            Stats.registerHttpService(holder);
            Stats.getWebMetric(holder, status.code()).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    Logger getLogger();

    void setResponseHeader(H responseHeaders, CharSequence name, Object value);

}
