package loghub.netty;

import java.util.List;
import java.util.Map;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

public interface DashboardService {
    String getPrefix();
    List<SimpleChannelInboundHandler<FullHttpRequest>> getServices(Map<String, Object> properties);
}
