package loghub;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.DateparsingCheck;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.NotFound;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;

public class DashboardHttpServer extends AbstractHttpServer {

    private final SimpleChannelInboundHandler<FullHttpRequest> ROOTREDIRECT = new RootRedirect();
    private final SimpleChannelInboundHandler<FullHttpRequest> NOTFOUND = new NotFound();
    private final SimpleChannelInboundHandler<FullHttpRequest> JMXPROXY = new JmxProxy();
    private final SimpleChannelInboundHandler<FullHttpRequest> DATAPARSINGCHECK = new DateparsingCheck();

    @Override
    public void addModelHandlers(ChannelPipeline p) {
        p.addLast(ROOTREDIRECT);
        p.addLast(new ResourceFiles());
        p.addLast(JMXPROXY);
        p.addLast(DATAPARSINGCHECK);
        p.addLast(NOTFOUND);
    }

}
