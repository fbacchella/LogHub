package loghub.netty;

import java.util.List;
import java.util.Map;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

public interface DashboardService {
    String getName();
    List<SimpleChannelInboundHandler<FullHttpRequest>> getHandlers(Map<String, Object> properties);
    default List<Runnable> getStarters() {
        return List.of();
    }
    default List<Runnable> getStoppers() {
        return List.of();
    }
}
