package loghub.netty.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer.Context;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import loghub.configuration.Properties;

public abstract class HttpRequestProcessing extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LogManager.getLogger();

    public class HttpRequestFailure extends Exception {
        public final HttpResponseStatus status;
        public final String message;
        public final Map<AsciiString, Object> additionHeaders;
        public HttpRequestFailure(HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
            this.status = status;
            this.message = message;
            this.additionHeaders = additionHeaders;
        }
        public HttpRequestFailure(HttpResponseStatus status, String message) {
            this.status = status;
            this.message = message;
            this.additionHeaders = Collections.emptyMap();
        }
    }

    private static final ThreadLocal<SimpleDateFormat> dateFormatter = ThreadLocal.withInitial(() -> {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormatter;
    });

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!(msg instanceof HttpRequest)) {
            return false;
        }
        return acceptRequest((HttpRequest)msg);
    }

    public abstract boolean acceptRequest(HttpRequest request);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        Properties.metrics.counter("WebServer.inflight").inc();
        try( Context tct = Properties.metrics.timer("WebServer.timer").time()) {
            if (!request.decoderResult().isSuccess()) {
                failure(ctx, request, BAD_REQUEST, "Can't decode request", Collections.emptyMap());
                return;
            }
            try {
                if (!processRequest(request, ctx)) {
                    // If keep-alive is off, close the connection once the content is fully written.
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
                }
                Properties.metrics.meter("WebServer.status.200").mark();
            } catch (HttpRequestFailure e) {
                failure(ctx, request, e.status, e.message, e.additionHeaders);
            }
        } finally {
            Properties.metrics.counter("WebServer.inflight").dec();
        }
    }

    protected abstract boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

    protected abstract String getContentType();

    /**
     * Return the origin date of the content, or null if irrelevant
     * @return the content date
     */
    protected abstract Date getContentDate();

    /**
     * Can be used to add custom handlers to a response, call by {@link #writeResponse(ChannelHandlerContext, FullHttpRequest, ByteBuf, int)}.
     * So if processRequest don't call it, no handlers will be added
     * 
     * @param request
     * @param response
     */
    protected void addCustomHeaders(HttpRequest request, HttpResponse response) {

    }

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
        addCustomHeaders(request, response);
        boolean keepAlive = addKeepAlive(request, response, length);
        ChannelFuture sendFileFuture = sender.get();
        addLogger(sendFileFuture, request.method().name(), request.uri(), response.status().code(), "completed");
        return keepAlive;
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

    protected void addContentDate(HttpRequest request, HttpResponse response) {
        Date contentDate = getContentDate();
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, dateFormatter.get().format(contentDate));
        }
    }

    protected void addContentType(HttpRequest request, HttpResponse response) {
        String contentType = getContentType();
        if (contentType != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
    }

    protected void failure(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
        logger.warn("{} {}: {} transfer complete: {}", () -> request.method(), () -> request.uri(), () -> status.code(), () -> message);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, 
                Unpooled.copiedBuffer(message + "\r\n", CharsetUtil.UTF_8));
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
            logger.error("Internal server error");
            logger.catching(Level.ERROR, cause);
        }
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer("Critical internal server error\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ChannelFuture sendFileFuture = ctx.writeAndFlush(response);
        sendFileFuture.addListener(ChannelFutureListener.CLOSE);
        Properties.metrics.meter("WebServer.status." + INTERNAL_SERVER_ERROR.code()).mark();;
    }
}
