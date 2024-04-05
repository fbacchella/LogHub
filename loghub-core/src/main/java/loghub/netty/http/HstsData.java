package loghub.netty.http;

import java.time.Duration;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data @Builder @Accessors(fluent = true)
public class HstsData {

    public static final  String HEADER_NAME = "Strict-Transport-Security";

    private class HSTSHandler extends ChannelDuplexHandler {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                HttpHeaders headers = response.headers();
                headers.set(HstsData.HEADER_NAME, HstsData.this.getHeader());
            }
            super.write(ctx, msg, promise);
        }
    }

    private final Duration ONE_YEAR = Duration.ofDays(365);
    private final Duration maxAge;
    private final boolean includeSubDomains;
    private final boolean preload;

    private HstsData(Duration maxAge, boolean includeSubDomains, boolean preload) {
        if (preload && maxAge.compareTo(ONE_YEAR) < 0) {
            throw new IllegalArgumentException("Duration must be more that one year with preload");
        }
        this.maxAge = maxAge;
        this.includeSubDomains = includeSubDomains || preload;
        this.preload = preload;
    }

    public String getHeader() {
        return String.format("max-age=%s%s%s",
                maxAge.getSeconds(),
                includeSubDomains ? "; includeSubDomains" : "",
                preload ? "; preload" : "" );
    }

    public ChannelHandlerAdapter getChannelHandler() {
        return new HSTSHandler();
    }

}
