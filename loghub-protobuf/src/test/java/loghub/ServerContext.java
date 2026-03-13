package loghub;

import java.net.URI;

import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.metrics.Stats;
import loghub.netty.transport.TcpTransport;
import loghub.netty.transport.TcpTransport.Builder;
import loghub.protobuf.GrpcStreamHandler;
import loghub.security.ssl.ClientAuthentication;

public class ServerContext {

    final HttpTestServer resource = new HttpTestServer();
    public final URI listenUri;

    public ServerContext(TlsContext ctx, GrpcStreamHandler.Factory factory) {
        resource.setModelHandlers();
        resource.setHttp2handler(ch -> ch.pipeline().addLast("GrpcStreamHandler", factory.get()));

        Builder transportConfig = TcpTransport.getBuilder();
        transportConfig.setEndpoint("localhost");
        transportConfig.setWithSsl(true);
        transportConfig.setSslContext(ctx.sslctx);
        transportConfig.setSslKeyAlias("localhost (loghub ca)");
        transportConfig.setSslClientAuthentication(ClientAuthentication.NONE);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
        transportConfig.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
        transportConfig.setThreadPrefix("gRPCServer");
        listenUri = resource.startServer(transportConfig);
    }

    public void resetStats() {
        Stats.reset();
        Stats.registerHttpService(resource.getHolder());
    }

    public void clean() {
        resource.after();
    }
}
