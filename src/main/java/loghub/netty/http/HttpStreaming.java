package loghub.netty.http;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import io.netty.util.CharsetUtil;

public abstract class HttpStreaming extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LogManager.getLogger();

    class HttpRequestFailure extends Exception {
        public final HttpResponseStatus status;
        public final String message;
        public HttpRequestFailure(HttpResponseStatus status, String message) {
            this.status = status;
            this.message = message;
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
        logger.debug("{} {}", () -> request.method(), () -> request.uri());
        if (!request.decoderResult().isSuccess()) {
            failure(ctx, request, BAD_REQUEST, "Can't decode request");
            return;
        }
        if (request.method() != GET) {
            failure(ctx, request, METHOD_NOT_ALLOWED, "Only GET allowed");
            return;
        }
        try {
            if (!processRequest(request, ctx)) {
                // If keep-alive is off, close the connection once the content is fully written.
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        } catch (HttpRequestFailure e) {
            failure(ctx, request, e.status, e.message);
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
     * Return the size of the content or -1 if unknown
     * 
     * @return the size of the content
     */
    protected abstract int getSize();

    /**
     * Can be used to add custom handlers to a response, call by {@link #writeResponse(ChannelHandlerContext, FullHttpRequest, ByteBuf)}.
     * So if processRequest don't call it, no handlers will be added
     * 
     * @param request
     * @param response
     */
    protected void addCustomHeaders(HttpRequest request, HttpResponse response) {

    }

    protected boolean writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ChunkedInput<ByteBuf> content) {
        return internalWriteResponse(ctx, request, new HttpChunkedInput(content, LastHttpContent.EMPTY_LAST_CONTENT));
    }

    protected boolean writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, ByteBuf content) {
        return internalWriteResponse(ctx, request, content);
    }

    private boolean internalWriteResponse(ChannelHandlerContext ctx, FullHttpRequest request, Object content) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        addContentDate(request, response);
        boolean keepAlive = addKeepAlive(request, response);
        addCustomHeaders(request, response);
        // Write the initial line and the header.
        ctx.write(response);
        ChannelFuture sendFileFuture = ctx.writeAndFlush(content);
        // Add a logging listener to the futur
        sendFileFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("{} {}: 200 transfer complete", () -> request.method(), () -> request.uri());
            }
        });

        return keepAlive;
    }


    protected boolean  addKeepAlive(HttpRequest request, HttpResponse response) {
        // Decide whether to close the connection or not.
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            int length = getSize();
            if (length >= 0) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, length);
                // Add keep alive header as per:
                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            } else {
                // Unknown size, can't keep alive
                keepAlive = false;
            }
        }
        return keepAlive;
    }

    protected void addContentDate(HttpRequest request, HttpResponse response) {
        Date contentDate = getContentDate();
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, dateFormatter.get().format(contentDate));
        }
    }

    protected void failure(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status, String message) {
        logger.warn("{} {}: {} transfer complete: {}", () -> request.method(), () -> request.uri(), () -> status.code(), () -> message);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, 
                Unpooled.copiedBuffer(message + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ChannelFuture sendFileFuture = ctx.writeAndFlush(response);
        sendFileFuture.addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        while(cause.getCause() != null) {
            cause = cause.getCause();
        }
        logger.error("Internal server error");
        logger.catching(Level.ERROR, cause);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer("Critical internal server error\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ChannelFuture sendFileFuture = ctx.writeAndFlush(response);
        sendFileFuture.addListener(ChannelFutureListener.CLOSE);
    }
}
