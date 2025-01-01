package loghub.prometheus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.prometheus.metrics.exporter.common.PrometheusHttpExchange;
import io.prometheus.metrics.exporter.common.PrometheusHttpRequest;
import io.prometheus.metrics.exporter.common.PrometheusHttpResponse;
import loghub.netty.http.HttpHandler;

class NettyPrometheusHttpExchange implements PrometheusHttpExchange {

    private class HttpRequest implements PrometheusHttpRequest {
        @Override
        public String getQueryString() {
            return uri.getQuery();
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            return Collections.enumeration(nettyRequest.headers().getAll(name));
        }

        @Override
        public String getMethod() {
            return nettyRequest.method().name();
        }

        /**
         * Absolute path of the HTTP request.
         */
        @Override
        public String getRequestPath() {
            return uri.getPath().replace("/prometheus/", "");
        }
    }

    private class HttpResponse implements PrometheusHttpResponse {
        @Override
        public void setHeader(String name, String value) {
            NettyPrometheusHttpExchange.this.headers.put(name, value);
        }

        @Override
        public OutputStream sendHeadersAndGetBody(int statusCode, int contentLength) {
            NettyPrometheusHttpExchange.this.status = statusCode;
            NettyPrometheusHttpExchange.this.content = new ByteArrayOutputStream(contentLength);
            return content;
        }
    }

    private final HttpRequest request = new HttpRequest();
    private final HttpResponse response = new HttpResponse();
    private final URI uri;
    private final Map<String, String> headers = new HashMap<>();
    int status;
    ByteArrayOutputStream content;

    private final FullHttpRequest nettyRequest;
    private final HttpHandler handler;
    private final ChannelHandlerContext ctx;

    NettyPrometheusHttpExchange(HttpHandler handler, FullHttpRequest nettyRequest, ChannelHandlerContext ctx) {
        this.nettyRequest = nettyRequest;
        this.uri = URI.create(nettyRequest.uri());
        this.handler = handler;
        this.ctx = ctx;
    }

    void copyHeader(HttpHeaders nettyHeaders) {
        for (Map.Entry<String, String> e: headers.entrySet()) {
            if (! nettyHeaders.contains(e.getKey() )) {
                nettyHeaders.add(e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public PrometheusHttpRequest getRequest() {
        return request;
    }

    @Override
    public PrometheusHttpResponse getResponse() {
        return response;
    }

    @Override
    public void handleException(IOException e) {
        try {
            handler.exceptionCaught(ctx, e);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void handleException(RuntimeException e) {
        try {
            handler.exceptionCaught(ctx, e);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        // Nothing to do
    }

}
