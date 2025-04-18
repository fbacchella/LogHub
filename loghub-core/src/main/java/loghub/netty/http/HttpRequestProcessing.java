package loghub.netty.http;

import java.util.function.Predicate;

import com.codahale.metrics.Meter;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.metrics.Stats;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public abstract class HttpRequestProcessing extends HttpHandler {

    protected HttpRequestProcessing() {
        super(true);
    }

    protected HttpRequestProcessing(Predicate<String> urlFilter) {
        super(true, urlFilter);
    }

    protected HttpRequestProcessing(Predicate<String> urlFilter, String... method) {
        super(true, urlFilter, method);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void subProcessing(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        processRequest(request, ctx);
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
        Stats.getMetric(Meter.class, "WebServer.status." + OK.code()).mark();
    }

    protected abstract void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure;

}
