package loghub.netty.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.AsciiString;
import loghub.Helpers;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Sharable
public abstract class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> implements HttpCommon<HttpRequest, HttpResponse> {

    protected final Logger logger;
    private final Predicate<String> urlFilter;
    private final Set<HttpMethod> methods;


    protected HttpHandler(boolean release) {
        super(release);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        RequestFilter filter = HttpCommon.RequestFilter.of(this);
        this.methods = filter.methods();
        this.urlFilter = filter.urlFilter();
    }

    protected HttpHandler(boolean release, Predicate<String> urlFilter, String... methods) {
        super(FullHttpRequest.class, release);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        this.urlFilter = urlFilter;
        this.methods = Arrays.stream(methods).map(i -> HttpMethod.valueOf(i.toUpperCase())).collect(Collectors.toSet());
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public void setResponseHeader(HttpResponse response, CharSequence name, Object value) {
        response.headers().set(name, value);
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
        if (super.acceptInboundMessage(msg) && msg instanceof HttpRequest request) {
            return acceptRequest(request);
        } else {
            return false;
        }
    }

    @Override
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
            addNoCacheHeaders(response);
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


    @Override
    public void addCustomHeadersCommon(HttpRequest request, HttpResponse response) {
        addCustomHeaders(request, response);
    }

    protected void addCustomHeaders(HttpRequest request, HttpResponse response) {
    }

    /**
     * Return the origin date of the content, or null if irrelevant
     * @return the content date
     * @deprecated use {@link #getContentDateInstant(io.netty.handler.codec.http.HttpRequest, io.netty.handler.codec.http.HttpResponse)} instead.
     */
    @Deprecated
    protected Date getContentDate(io.netty.handler.codec.http.HttpRequest request, io.netty.handler.codec.http.HttpResponse response) {
        Instant i = getContentDateInstant(request, response);
        return i != null ? Date.from(i) : null;
    }

    @Override
    public Instant getContentDateInstant(HttpRequest request, HttpResponse response) {
        return Instant.now();
    }

    @Override
    public void addContentDateCommon(HttpRequest request, HttpResponse response) {
        addContentDate(request, response);
    }

    protected void addContentDate(HttpRequest request, HttpResponse response) {
        Instant contentDate = getContentDateInstant(request, response);
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, HttpCommon.DATE_FORMATTER.print(contentDate));
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
        } else if (Optional.ofNullable(cause.getCause()).orElse(cause) instanceof HttpRequestFailure failure) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                                                                    HTTP_1_1, failure.status,
                                                                    Unpooled.copiedBuffer(failure.message + "\r\n", StandardCharsets.UTF_8));
            HttpUtil.setKeepAlive(response, failure.status.code() < 500);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            failure.additionHeaders.forEach((key, value) -> response.headers().add(key, value));
            ctx.writeAndFlush(response);
            doStatusMetric(ctx, failure.status);
        } else {
            logger.atError()
                  .withThrowable(cause)
                  .log("Internal server error: {}", () -> Helpers.resolveThrowableException(cause));
            logger.catching(Level.ERROR, cause);
            FullHttpResponse response = new DefaultFullHttpResponse(
                                                                    HTTP_1_1, SERVICE_UNAVAILABLE, Unpooled.copiedBuffer("Critical internal server error\r\n", StandardCharsets.UTF_8));
            HttpUtil.setKeepAlive(response, false);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            ctx.writeAndFlush(response);
            doStatusMetric(ctx, SERVICE_UNAVAILABLE);
        }
    }

}
