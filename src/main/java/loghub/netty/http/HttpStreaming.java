package loghub.netty.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;

public abstract class HttpStreaming extends SimpleChannelInboundHandler<FullHttpRequest> {

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
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        try {
            if (!processRequest(request, ctx)) {
                // If keep-alive is off, close the connection once the content is fully written.
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        } catch (HttpRequestFailure e) {
            failure(ctx, e.status, e.message);
        }
    }

    protected abstract boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

    protected abstract String getContentType();

    /**
     * Return the origin date of the content, or null if irrelevent
     * @return the content date
     */
    protected abstract Date getContentDate();

    /**
     * Return the size of the content or -1 if unknown
     * @return the size of the content
     */
    protected abstract long getSize();

    protected boolean writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, Object content) {
        // Build the response object.
        DefaultHttpResponse  response = new DefaultHttpResponse(
                HTTP_1_1, request.decoderResult().isSuccess()? OK : BAD_REQUEST);

        String contentType = getContentType();
        if (contentType != null && !contentType.isEmpty()) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }

        // Decide whether to close the connection or not.
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            long length = getSize();
            if (length > 0) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, (int)length);
                // Add keep alive header as per:
                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            } else {
                // Unknown size, can't keep alive
                keepAlive = false;
            }
        }

        Date contentDate = getContentDate();
        if (contentDate != null) {
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, dateFormatter.get().format(contentDate));
        }
        // Write the response header.
        ctx.write(response);

        // Write the content
        ctx.writeAndFlush(content);

        return keepAlive;
    }

    protected void failure(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        //cause.printStackTrace();
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, 
                Unpooled.copiedBuffer(message + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        while(cause.getCause() != null) {
            cause = cause.getCause();
        }
        cause.printStackTrace();
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, INTERNAL_SERVER_ERROR);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
