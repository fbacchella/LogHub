package loghub;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.configuration.Properties;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.NotFound;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;

public class DashboardHttpServer extends AbstractHttpServer {

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> NOTFOUND = new NotFound();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();

    public boolean configure(Properties properties) {
        // DashboardHttpServer is it's own channel consumer
        return super.configure(properties, this);
    }

    @Override
    public void addModelHandlers(ChannelPipeline p) {
        p.addLast(ROOTREDIRECT);
        p.addLast(new ResourceFiles());
        p.addLast(JMXPROXY);
        p.addLast(NOTFOUND);
    }

}
