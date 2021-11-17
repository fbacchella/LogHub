package loghub.netty.http;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class GraphMetric extends HttpRequestProcessing implements ChannelInboundHandler {

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/graph/");
    }

    @Override
    protected void processRequest(FullHttpRequest request,
                                  ChannelHandlerContext ctx)
                                                  throws HttpRequestFailure {
        writeResponse(ctx, request, HttpResponseStatus.MOVED_PERMANENTLY, Unpooled.EMPTY_BUFFER, 0);
    }

    @Override
    protected void addCustomHeaders(HttpRequest request,
                                    HttpResponse response) {
        try {
            String path = request.uri().replace("/graph", "");
            URI uri = new URI(path);
            response.headers().set(HttpHeaderNames.LOCATION, "/static/index.html?q=" + URLEncoder.encode(uri.getPath(), "UTF-8"));
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            //Not reachable
        }
    }

}
