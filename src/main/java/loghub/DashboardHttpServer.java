package loghub;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.JwtToken;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;
import loghub.netty.http.TokenFilter;
import loghub.security.AuthenticationHandler;

public class DashboardHttpServer extends AbstractHttpServer {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder extends AbstractHttpServer.Builder<DashboardHttpServer> {
        @Override
        public DashboardHttpServer build() {
            return new DashboardHttpServer(this);
        }
    }

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();
    private final SimpleChannelInboundHandler<FullHttpRequest> TOKENGENERATOR;
    private final SimpleChannelInboundHandler<FullHttpRequest> TOKENFILTER;

    private DashboardHttpServer(Builder builder) {
        super(builder);
        AuthenticationHandler authHandler = getAuthHandler();
        if (authHandler != null && authHandler.isWithJwt()) {
            TOKENGENERATOR = new JwtToken(authHandler.getJwtHandler());
            TOKENFILTER = new TokenFilter(authHandler);
        } else {
            TOKENGENERATOR = null;
            TOKENFILTER = null;
        }
    }

    public void start() {
        configure(this);
    }
    
    @Override
    public void addModelHandlers(ChannelPipeline p) {
        p.addLast(ROOTREDIRECT);
        p.addLast(new ResourceFiles());
        p.addLast(JMXPROXY);
        if (TOKENGENERATOR != null && TOKENFILTER != null) {
            p.addLast(TOKENFILTER);
            p.addLast(TOKENGENERATOR);
        }
    }

}
