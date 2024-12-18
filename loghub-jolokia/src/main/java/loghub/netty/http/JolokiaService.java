package loghub.netty.http;

import java.util.Map;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.jolokia.JolokiaEndpoint;

/**
 * A compatibility class, kept for pre-service era.
 * @deprecated
 */
@Deprecated
public class JolokiaService {

    // Kept for compatibility
    public static SimpleChannelInboundHandler<FullHttpRequest> of(Map<String, Object> properties) {
        JolokiaEndpoint.Builder builder = JolokiaEndpoint.getBuilder();
        if (properties.containsKey("policyLocation")) {
            builder.setPolicyLocation(properties.get("policyLocation").toString());
        }
        return builder.build();
    }

    private JolokiaService() {
        // Kept empty on purpose
    }

}
