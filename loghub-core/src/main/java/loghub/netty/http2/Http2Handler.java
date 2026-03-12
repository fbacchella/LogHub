package loghub.netty.http2;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AsciiString;
import loghub.Helpers;
import loghub.netty.http.HttpCommon;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.NoCache;
import loghub.netty.http.NotSharable;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;

@Sharable
public abstract class Http2Handler extends SimpleChannelInboundHandler<Http2HeadersFrame> implements HttpCommon<Http2HeadersFrame, Http2Headers> {

    protected final Logger logger;
    private final Predicate<String> urlFilter;
    private final Set<HttpMethod> methods;


    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public void setResponseHeader(Http2Headers responseHeaders, CharSequence name, Object value) {
        responseHeaders.set(name, value.toString());
    }

    protected Http2Handler() {
        this(true);
    }

    protected Http2Handler(boolean release) {
        super(release);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        RequestFilter filter = HttpCommon.RequestFilter.of(this);
        this.methods = filter.methods();
        this.urlFilter = filter.urlFilter();
    }

    @Override
    public boolean isSharable() {
        return getClass().getAnnotation(NotSharable.class) == null && super.isSharable();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http2HeadersFrame frame) {
        try {
            subProcessing(frame, ctx);
        } catch (HttpRequestFailure e) {
            failure(ctx, frame, e.status, e.message, e.additionHeaders);
        }
    }

    @Override
    public final boolean acceptInboundMessage(Object msg) throws Exception {
        if (super.acceptInboundMessage(msg) && msg instanceof Http2HeadersFrame frame) {
            return acceptRequest(frame);
        } else {
            return false;
        }
    }

    @Override
    public boolean acceptRequest(Http2HeadersFrame headers) {
        CharSequence method = headers.headers().method();
        CharSequence path = headers.headers().path();
        return method != null && methods.contains(HttpMethod.valueOf(method.toString().toUpperCase()))
                && path != null && urlFilter.test(path.toString());
    }

    protected abstract void subProcessing(Http2HeadersFrame frame, ChannelHandlerContext ctx) throws HttpRequestFailure;

    protected void writeResponse(ChannelHandlerContext ctx, Http2HeadersFrame requestFrame, ByteBuf content, int length) {
        writeResponse(ctx, requestFrame, OK, content, length);
    }

    protected void writeResponse(ChannelHandlerContext ctx, Http2HeadersFrame requestFrame, HttpResponseStatus status, ByteBuf content, int length) {
        Http2Headers responseHeaders = new DefaultHttp2Headers().status(status.codeAsText());

        addContentDate(requestFrame, responseHeaders, ctx);
        addContentType(requestFrame, responseHeaders, ctx);
        if (getClass().getAnnotation(NoCache.class) != null) {
            addNoCacheHeaders(responseHeaders);
        }
        if (length >= 0) {
            responseHeaders.setInt(HttpHeaderNames.CONTENT_LENGTH, length);
        }
        addCustomHeaders(requestFrame, responseHeaders, ctx);

        ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
        ChannelFuture lastFuture = ctx.writeAndFlush(new DefaultHttp2DataFrame(content, true));

        addLogger(lastFuture, requestFrame.headers().method().toString(), requestFrame.headers().path().toString(), status.code(), "completed");
        doStatusMetric(ctx, status);
    }


    private void failure(ChannelHandlerContext ctx, Http2HeadersFrame requestFrame, HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
        Http2Headers headers = requestFrame.headers();
        logger.warn("{} {}: {} transfer complete: {}", headers::method, headers::path, status::code, () -> message);
        Http2Headers responseHeaders = new DefaultHttp2Headers().status(status.codeAsText());
        responseHeaders.set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
        ByteBuf content = Unpooled.copiedBuffer(message + "\r\n", StandardCharsets.UTF_8);
        responseHeaders.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        additionHeaders.forEach((key, value) -> responseHeaders.add(key, value.toString()));
        ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
        ctx.writeAndFlush(new DefaultHttp2DataFrame(content, true));
        doStatusMetric(ctx, status);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (Optional.ofNullable(cause.getCause()).orElse(cause) instanceof HttpRequestFailure failure) {
            Http2Headers responseHeaders = new DefaultHttp2Headers().status(failure.status.codeAsText());
            responseHeaders.set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            ByteBuf content = Unpooled.copiedBuffer(failure.message + "\r\n", StandardCharsets.UTF_8);
            responseHeaders.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
            failure.additionHeaders.forEach((key, value) -> responseHeaders.add(key.toString(), value.toString()));
            ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
            ctx.writeAndFlush(new DefaultHttp2DataFrame(content, true));
            doStatusMetric(ctx, failure.status);
        } else {
            logger.atError()
                  .withThrowable(cause)
                  .log("Internal server error: {}", () -> Helpers.resolveThrowableException(cause));
            logger.catching(Level.ERROR, cause);
            Http2Headers responseHeaders = new DefaultHttp2Headers().status(SERVICE_UNAVAILABLE.codeAsText());
            responseHeaders.set(HttpHeaderNames.CONTENT_TYPE, TEXT_CONTENT_TYPE);
            ByteBuf content = Unpooled.copiedBuffer("Critical internal server error\r\n", StandardCharsets.UTF_8);
            responseHeaders.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
            ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
            ctx.writeAndFlush(new DefaultHttp2DataFrame(content, true));
            doStatusMetric(ctx, SERVICE_UNAVAILABLE);
        }
    }


    @Override
    public void addCustomHeadersCommon(Http2HeadersFrame requestFrame, Http2Headers responseHeaders) {
        addCustomHeaders(requestFrame, responseHeaders);
    }

    protected void addCustomHeaders(Http2HeadersFrame requestFrame, Http2Headers responseHeaders) {
    }

    @Override
    public void addContentDateCommon(Http2HeadersFrame requestFrame, Http2Headers responseHeaders) {
        addContentDate(requestFrame, responseHeaders);
    }

    protected void addContentDate(Http2HeadersFrame requestFrame, Http2Headers responseHeaders) {
        Instant contentDate = getContentDateInstant(requestFrame, responseHeaders);
        if (contentDate != null) {
            responseHeaders.set(HttpHeaderNames.LAST_MODIFIED, HttpCommon.DATE_FORMATTER.print(contentDate));
        }
    }

}
