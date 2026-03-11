package loghub.netty.http;

import java.time.Duration;
import java.util.Map;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import loghub.VarFormatter;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class HstsData {

    public static final  String HEADER_NAME = "Strict-Transport-Security";
    private static final Duration ONE_YEAR = Duration.ofDays(365);
    private static final VarFormatter FORMATTER = new VarFormatter("max-age=${max-age}${includeSubDomains}${preload}");

    private class HSTSHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            switch (msg) {
                case HttpResponse response -> {
                    HttpHeaders headers = response.headers();
                    headers.set(HstsData.HEADER_NAME, HstsData.this.getHeader());
                }
                case Http2HeadersFrame frame -> {
                    Http2Headers headers = frame.headers();
                    headers.set(HstsData.HEADER_NAME, HstsData.this.getHeader());
                }
                default -> { }
            }
            super.write(ctx, msg, promise);
        }
    }

    private final Duration maxAge;
    private final boolean includeSubDomains;
    private final boolean preload;

    @Builder
    private HstsData(Duration maxAge, boolean includeSubDomains, boolean preload) {
        if (preload && maxAge.compareTo(ONE_YEAR) < 0) {
            throw new IllegalArgumentException("Duration must be more that one year with preload");
        }
        this.maxAge = maxAge;
        this.includeSubDomains = includeSubDomains || preload;
        this.preload = preload;
    }

    public String getHeader() {
        return FORMATTER.format(Map.of("max-age", maxAge.getSeconds(),
                                       "includeSubDomains", includeSubDomains ? "; includeSubDomains" : "",
                                       "preload", preload ? "; preload" : ""));
    }

    public ChannelHandler getChannelHandler() {
        return new HSTSHandler();
    }

}
