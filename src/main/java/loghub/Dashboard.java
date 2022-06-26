package loghub;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.netty.http.GetMetric;
import loghub.netty.http.GraphMetric;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.JwtToken;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;
import loghub.netty.http.TokenFilter;
import loghub.netty.servers.HttpServer;
import lombok.experimental.Accessors;

public class Dashboard extends HttpServer {

    @Accessors(chain=true)
    public static class Builder extends HttpServer.Builder<Dashboard> {
        public Dashboard build() {
            return new Dashboard(this);
        }
    }

    public static Dashboard.Builder getBuilder() {
        return new Dashboard.Builder();
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();
    private final SimpleChannelInboundHandler<FullHttpRequest> GETMETRIC = new GetMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> GRAPHMETRIC = new GraphMetric();
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenGenerator;
    private final SimpleChannelInboundHandler<FullHttpRequest> tokenFilter;

    private Dashboard(Builder builder) {
        super(builder);
        if (config.getAuthHandler() != null && config.getAuthHandler().getJwtHandler() != null) {
            tokenGenerator = new JwtToken(config.getAuthHandler().getJwtHandler());
            tokenFilter = new TokenFilter(config.getAuthHandler());
        } else {
            tokenGenerator = null;
            tokenFilter = null;
        }
        config.setThreadPrefix("Dashboard");
    }

    @Override
    public void addModelHandlers(ChannelPipeline p) {
        p.addLast(ROOTREDIRECT);
        p.addLast(new ResourceFiles());
        p.addLast(JMXPROXY);
        p.addLast(GETMETRIC);
        p.addLast(GRAPHMETRIC);
        if (tokenGenerator != null && tokenFilter != null) {
            p.addLast(tokenFilter);
            p.addLast(tokenGenerator);
        }
    }

}
