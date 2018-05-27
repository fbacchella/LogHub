package loghub;

import java.util.Map;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import loghub.configuration.Properties;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.JmxProxy;
import loghub.netty.http.JwtToken;
import loghub.netty.http.ResourceFiles;
import loghub.netty.http.RootRedirect;
import loghub.netty.http.TokenFilter;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;

public class DashboardHttpServer extends AbstractHttpServer<DashboardHttpServer, DashboardHttpServer.Builder> {

    public static DashboardHttpServer.Builder getBuilder() {
        return new DashboardHttpServer.Builder();
    }

    public static class Builder extends AbstractHttpServer.Builder<DashboardHttpServer, DashboardHttpServer.Builder> {
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

    public static DashboardHttpServer.Builder buildDashboad(Map<Object, Object> collect, Properties props) {
        int port = (Integer) collect.compute("port", (i,j) -> {
            if (j != null && ! (j instanceof Integer)) {
                throw new IllegalArgumentException("http dasbhoard port is not an integer");
            }
            return j != null ? j : -1;
        });
        if (port < 0) {
            return null;
        } else {
            boolean useSSL;
            String clientAuthentication;
            String sslKeyalias;
            if (Boolean.TRUE.equals(collect.get("withSSL"))) {
                useSSL = true;
                clientAuthentication = collect.compute("SSLClientAuthentication", (i, j) -> j != null ? j : ClientAuthentication.NOTNEEDED).toString();
                sslKeyalias = (String) collect.get("SSLKeyAlias");
            } else {
                useSSL = false;
                clientAuthentication = ClientAuthentication.NONE.name();
                sslKeyalias = null;
            }
            AuthenticationHandler authHandler = AuthenticationHandler.getBuilder()
                    .useSsl(useSSL).setSslClientAuthentication(clientAuthentication)
                    .useJwt((Boolean) collect.compute("jwt", (i,j) -> Boolean.TRUE.equals(j))).setJwtHandler(props.jwtHandler)
                    .setJaasName(collect.compute("jaasName", (i,j) -> j != null ? j : "").toString()).setJaasConfig(props.jaasConfig)
                    .build();
            return getBuilder()
                    .setPort(port)
                    .setSSLContext(props.ssl).useSSL(useSSL).setSSLKeyAlias(sslKeyalias)
                    .setAuthHandler(authHandler);
        }
    }

}
