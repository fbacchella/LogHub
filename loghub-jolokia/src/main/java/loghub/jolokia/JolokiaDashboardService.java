package loghub.jolokia;

import java.util.List;
import java.util.Map;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.netty.DashboardService;

public class JolokiaDashboardService implements DashboardService {

    @Override
    public String getName() {
        return "jolokia";
    }

    @Override
    public List<SimpleChannelInboundHandler<FullHttpRequest>> getHandlers(Map<String, Object> properties) {
        JolokiaEndpoint.Builder builder = JolokiaEndpoint.getBuilder();
        if (properties.containsKey("policyLocation")) {
            builder.setPolicyLocation(properties.get("policyLocation").toString());
        }
        JolokiaEndpoint ps = builder.build();
        return List.of(ps);
    }

}
