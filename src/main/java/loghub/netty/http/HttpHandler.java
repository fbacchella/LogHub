package loghub.netty.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.AsciiString;
import loghub.Helpers;
import loghub.configuration.Properties;

@Sharable
public abstract class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    protected final Logger logger;
    private final Predicate<String> urlFilter;
    private final Set<HttpMethod> methods;

    private static final ThreadLocal<SimpleDateFormat> dateFormatter = ThreadLocal.withInitial(() -> {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormatter;
    });

    public HttpHandler(boolean release) {
        super(release);
        logger = LogManager.getLogger(Helpers.getFistInitClass());
        RequestAccept mask = getClass().getAnnotation(RequestAccept.class);
        if ( mask != null) {
            String filter = mask.filter();
            String path = mask.path();
            if (filter != null && ! filter.isEmpty()) {
                this.urlFilter = Pattern.compile(filter).asPredicate();
            } else if (filter != null && ! filter.isEmpty()) {
                this.urlFilter = i -> path.equals(i);
            } else {
                this.urlFilter = i -> true;
            }
            this.methods = Arrays.stream(mask.methods()).map( i -> HttpMethod.valueOf(i.toUpperCase())).collect(Collectors.toSet());
        } else {
            this.urlFilter = i -> true;
            this.methods = Collections.emptySet();
        }
    }

    public HttpHandler(boolean release, Predicate<String> urlFilter, String... methods) {
        super(FullHttpRequest.class, release);
        logger = LogManager.getLogger(Helpers.getFistInitClass());
        this.urlFilter = urlFilter;
        this.methods = Arrays.stream(methods).map( i -> HttpMethod.valueOf(i.toUpperCase())).collect(Collectors.toSet());
    }

    public HttpHandler(boolean release, Predicate<String> urlFilter) {
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
        return super.acceptInboundMessage(msg) && acceptRequest((HttpRequest)msg);
    }

    public boolean acceptRequest(HttpRequest request) {
        return methods.contains(request.method()) && urlFilter.test(request.uri());
    }

    protected abstract void subProcessing(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

    protected boolean writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ChunkedInput<ByteBuf> content, int length) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        return internalWriteResponse(ctx, request, response, length, () -> {
            HttpChunkedInput chunked = new HttpChunkedInput(content, LastHttpContent.EMPTY_LAST_CONTENT);
            ctx.write(response);
            return ctx.writeAndFlush(chunked);
        });
    }

    protected boolean writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ByteBuf content, int length) {
        // Build the response object.
        HttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content);

        return internalWriteResponse(ctx, request, response, length, () -> {
            return ctx.writeAndFlush(response);
        });
    }

    private boolean internalWriteResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response, int length, Supplier<ChannelFuture> sender) {
        addContentDate(request, response);
        addContentType(request, response);
        if (getClass().getAnnotation(NoCache.class) != null) {
            addNoCacheHeaders(request, response);
        }
        addCustomHeaders(request, response);
        boolean keepAlive = addKeepAlive(request, response, length);
        ChannelFuture sendFileFuture = sender.get();
        addLogger(sendFileFuture, request.method().name(), request.uri(), response.status().code(), "completed");
        return keepAlive;
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
        sendFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("{} {}: {} {}", method, uri, status, message);
            }
        });
    }

    protected boolean addKeepAlive(HttpRequest request, HttpResponse response, int length) {
        // Decide whether to close the connection or not.
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (length >= 0) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, length);
        } else {
            // Unknown size, can't keep alive
            keepAlive = false;
        }
        if (keepAlive) {
            // Add keep alive header as per:
            // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        } else {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }
        return keepAlive;
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


    protected void addContentDate(HttpRequest request, HttpResponse response) {
        Date contentDate = getContentDate(request, response);
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, dateFormatter.get().format(contentDate));
        }
    }

    protected String getContentType(HttpRequest request, HttpResponse response) {
        ContentType ct = getClass().getAnnotation(ContentType.class);
        if ( ct != null) {
            return ct.value();
        } else {
            return null;
        }

    }

    private void addContentType(HttpRequest request, HttpResponse response) {
        String contentType = getContentType(request, response);
        if (contentType != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
    }

    private void failure(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
        logger.warn("{} {}: {} transfer complete: {}", () -> request.method(), () -> request.uri(), () -> status.code(), () -> message);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, 
                Unpooled.copiedBuffer(message + "\r\n", StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        additionHeaders.entrySet().forEach( i-> response.headers().add(i.getKey(), i.getValue()));
        ChannelFuture sendFileFuture = ctx.writeAndFlush(response);
        sendFileFuture.addListener(ChannelFutureListener.CLOSE);
        Properties.metrics.meter("WebServer.status." + status.code()).mark();;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        while(cause.getCause() != null) {
            cause = cause.getCause();
        }
        //Network error, not a big deal
        if ( cause instanceof IOException) {
            logger.error("Network error: {}", cause.getMessage());
        } else {
            logger.error("Internal server errorr: {}", cause.getMessage());
            logger.catching(Level.ERROR, cause);
        }
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer("Critical internal server error\r\n", StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ChannelFuture sendFileFuture = ctx.writeAndFlush(response);
        sendFileFuture.addListener(ChannelFutureListener.CLOSE);
        Properties.metrics.meter("WebServer.status." + INTERNAL_SERVER_ERROR.code()).mark();;
    }

}
