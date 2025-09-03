package loghub.netty.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.AsciiString;
import loghub.Helpers;
import loghub.metrics.Stats;
import loghub.netty.HttpChannelConsumer;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Sharable
public abstract class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    protected final Logger logger;
    private final Predicate<String> urlFilter;
    private final Set<HttpMethod> methods;

    private static final ThreadLocal<SimpleDateFormat> dateFormatter = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    });

    protected HttpHandler(boolean release) {
        super(release);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        RequestAccept mask = getClass().getAnnotation(RequestAccept.class);
        if (mask != null) {
            String filter = mask.filter();
            String path = mask.path();
            if (filter != null && ! filter.isEmpty()) {
                this.urlFilter = Pattern.compile(filter).asPredicate();
            } else if (path != null && ! path.isEmpty()) {
                this.urlFilter = path::equals;
            } else {
                this.urlFilter = i -> true;
            }
            this.methods = Arrays.stream(mask.methods()).map(i -> HttpMethod.valueOf(i.toUpperCase())).collect(Collectors.toSet());
        } else {
            this.urlFilter = i -> true;
            this.methods = Collections.emptySet();
        }
    }

    protected HttpHandler(boolean release, Predicate<String> urlFilter, String... methods) {
        super(FullHttpRequest.class, release);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        this.urlFilter = urlFilter;
        this.methods = Arrays.stream(methods).map(i -> HttpMethod.valueOf(i.toUpperCase())).collect(Collectors.toSet());
    }

    protected HttpHandler(boolean release, Predicate<String> urlFilter) {
        this(release, urlFilter, "GET");
    }

    @Override
    public boolean isSharable() {
        return getClass().getAnnotation(NotSharable.class) == null && super.isSharable();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (!request.decoderResult().isSuccess()) {
            failure(ctx, request, BAD_REQUEST, "Can't decode request", Collections.emptyMap());
            return;
        }
        try {
            subProcessing(request, ctx);
        } catch (HttpRequestFailure e) {
            failure(ctx, request, e.status, e.message, e.additionHeaders);
        }
    }

    @Override
    public final boolean acceptInboundMessage(Object msg) throws Exception {
        return super.acceptInboundMessage(msg) && acceptRequest((HttpRequest) msg);
    }

    public boolean acceptRequest(HttpRequest request) {
        return methods.contains(request.method()) && urlFilter.test(request.uri());
    }

    protected abstract void subProcessing(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

    protected void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ChunkedInput<ByteBuf> content, int length) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        internalWriteResponse(ctx, request, response, length, () -> {
            HttpChunkedInput chunked = new HttpChunkedInput(content, LastHttpContent.EMPTY_LAST_CONTENT);
            ctx.write(response);
            return ctx.writeAndFlush(chunked);
        });
    }

    protected void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ByteBuf content, int length) {
        writeResponse(ctx, request, OK, content, length);
    }

    protected void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status, ByteBuf content, int length) {
        // Build the response object.
        HttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);

        internalWriteResponse(ctx, request, response, length, () -> ctx.writeAndFlush(response));
    }

    private void internalWriteResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response, int length, Supplier<ChannelFuture> sender) {
        addContentDate(request, response, ctx);
        addContentType(request, response, ctx);
        if (getClass().getAnnotation(NoCache.class) != null) {
            addNoCacheHeaders(request, response);
        }
        if (length >= 0) {
            HttpUtil.setContentLength(response, length);
        }
        HttpUtil.setKeepAlive(response, true);
        addCustomHeaders(request, response, ctx);
        ChannelFuture sendFileFuture = sender.get();
        addLogger(sendFileFuture, request.method().name(), request.uri(), response.status().code(), "completed");
        doStatusMetric(ctx, response.status());
    }

    private void addNoCacheHeaders(HttpRequest request, HttpResponse response) {
        response.headers().add(HttpHeaderNames.CACHE_CONTROL, "private, max-age=0");
        response.headers().add(HttpHeaderNames.EXPIRES, "-1");
    }

    /**
     * Used to log a status line when transfer is finished
     * @param sendFuture
     * @param method
     * @param uri
     * @param status
     * @param message
     */
    protected void addLogger(ChannelFuture sendFuture, String method, String uri, int status, String message) {
        sendFuture.addListener(
                (ChannelFutureListener) future -> logger.info("{} {}: {} {}", method, uri, status, message));
    }

    /**
     * Can be used to add custom handlers to a response, call by {@link #writeResponse(ChannelHandlerContext, FullHttpRequest, ByteBuf, int)}.
     * So if processRequest don't call it, no handlers will be added
     *
     * @param request
     * @param response
     * @param ctx
     */
    protected void addCustomHeaders(HttpRequest request, HttpResponse response, ChannelHandlerContext ctx) {
        addCustomHeaders(request, response);
    }

    /**
     * Can be used to add custom handlers to a response, call by {@link #writeResponse(ChannelHandlerContext, FullHttpRequest, ByteBuf, int)}.
     * So if processRequest don't call it, no handlers will be added
     *
     * @param request
     * @param response
     */
    protected void addCustomHeaders(HttpRequest request, HttpResponse response) {

    }

    /**
     * Return the origin date of the content, or null if irrelevant
     * @return the content date
     */
    protected Date getContentDate(io.netty.handler.codec.http.HttpRequest request, io.netty.handler.codec.http.HttpResponse response) {
        return new Date();
    }

    protected void addContentDate(FullHttpRequest request, HttpResponse response, ChannelHandlerContext ctx) {
        addContentDate(request, response);
    }

    protected void addContentDate(HttpRequest request, HttpResponse response) {
        Date contentDate = getContentDate(request, response);
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, dateFormatter.get().format(contentDate));
        }
    }

    protected String getContentType(HttpRequest request, HttpResponse response, ChannelHandlerContext ctx) {
        return getContentType(request, response);
    }

    protected String getContentType(HttpRequest request, HttpResponse response) {
        ContentType ct = getClass().getAnnotation(ContentType.class);
        if (ct != null) {
            return ct.value();
        } else {
            return null;
        }
    }

    private void addContentType(HttpRequest request, HttpResponse response, ChannelHandlerContext ctx) {
        String contentType = getContentType(request, response, ctx);
        if (contentType != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
    }

    private void failure(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
        logger.warn("{} {}: {} transfer complete: {}", request::method, request::uri, status::code, () -> message);
        FullHttpResponse response = new DefaultFullHttpResponse(
                                                                HTTP_1_1, status,
                                                                Unpooled.copiedBuffer(message + "\r\n", StandardCharsets.UTF_8));
        HttpUtil.setKeepAlive(response, status.code() < 500);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
        additionHeaders.forEach((key, value) -> response.headers().add(key, value));
        ctx.writeAndFlush(response);
        doStatusMetric(ctx, status);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // Forward non HTTP error
        if (cause instanceof IOException || cause.getCause() instanceof IOException) {
            ctx.fireExceptionCaught(cause);
        } else if (Optional.ofNullable(cause.getCause()).orElse(cause) instanceof HttpRequestFailure) {
            HttpRequestFailure failure = (HttpRequestFailure) Optional.ofNullable(cause.getCause()).orElse(cause);
            FullHttpResponse response = new DefaultFullHttpResponse(
                                                                    HTTP_1_1, failure.status,
                                                                    Unpooled.copiedBuffer(failure.message + "\r\n", StandardCharsets.UTF_8));
            HttpUtil.setKeepAlive(response, failure.status.code() < 500);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            failure.additionHeaders.forEach((key, value) -> response.headers().add(key, value));
            ctx.writeAndFlush(response);
            doStatusMetric(ctx, failure.status);
        } else {
            logger.error("Internal server error: {}", () -> Helpers.resolveThrowableException(cause));
            logger.catching(Level.ERROR, cause);
            FullHttpResponse response = new DefaultFullHttpResponse(
                                                                    HTTP_1_1, SERVICE_UNAVAILABLE, Unpooled.copiedBuffer("Critical internal server error\r\n", StandardCharsets.UTF_8));
            HttpUtil.setKeepAlive(response, false);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            ctx.writeAndFlush(response);
            doStatusMetric(ctx, SERVICE_UNAVAILABLE);
        }
    }

    private void doStatusMetric(ChannelHandlerContext ctx, HttpResponseStatus status) {
        Object holder = ctx.channel().attr(HttpChannelConsumer.HOLDERATTRIBUTE).get();
        long startTime = ctx.channel().attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
        Stats.getWebMetric(holder, status.code()).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }

}
