package loghub.receivers;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.net.ssl.SSLEngine;

import io.netty.channel.Channel;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.BuildableConnectionContext;
import loghub.BuilderClass;
import loghub.grpc.GrpcProcessor;
import loghub.grpc.GrpcService;
import loghub.grpc.GrpcHeaderHandler;
import loghub.events.Event;
import loghub.grpc.GrpcStats;
import loghub.grpc.GrpcStreamHandler;
import loghub.metrics.Stats;
import loghub.netty.ChannelConsumer;
import loghub.netty.ConsumerProvider;
import loghub.netty.ContextExtractor;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.NettyReceiver;
import loghub.netty.http.HttpProtocolVersion;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TRANSPORT;
import lombok.Getter;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

@Blocking
@SelfDecoder
@BuilderClass(GrpcReceiver.Builder.class)
public class GrpcReceiver
        extends NettyReceiver<GrpcReceiver, Http2Frame, GrpcReceiver.Builder>
        implements ConsumerProvider {

     public static class Builder
             extends NettyReceiver.Builder<GrpcReceiver, Http2Frame, GrpcReceiver.Builder> {
         @Setter
         GrpcProcessor[] grpcCodecs;
         public Builder() {
             setTransport(TRANSPORT.TCP);
             setWithSSL(true);
         }

         @Override
         public GrpcReceiver build() {
             return new GrpcReceiver(this);
         }
     }

     public static Builder getBuilder() {
         return new Builder();
     }

    @Getter
    private final GrpcHeaderHandler<GrpcReceiver> dispatcher;
    private final ContextExtractor<GrpcReceiver, Http2Frame, GrpcReceiver.Builder> resolver;
    private final Set<Channel> activeChannels = ConcurrentHashMap.newKeySet();

    protected GrpcReceiver(Builder builder) {
        super(builder);
        this.resolver = new ContextExtractor<>(Http2Frame.class, this);
        dispatcher = new GrpcHeaderHandler(new GrpcStats(this), this, builder.grpcCodecs);
    }

    @Override
    public void run() {
        Stats.registerHttpService(this);
        super.run();
    }

    @Override
    protected void tweakNettyBuilder(Builder builder, NettyTransport.Builder<?, Http2Frame, ?, ?> nettyTransportBuilder) {
        super.tweakNettyBuilder(builder, nettyTransportBuilder);
        if (nettyTransportBuilder instanceof AbstractIpTransport.Builder<?, ?, ?> ipTransportBuilder) {
            ipTransportBuilder.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
            ipTransportBuilder.setAlpnSelector(this::alpnSelector);
        }
    }

    public String alpnSelector(SSLEngine e, List<String> l) {
        if (l.contains(HttpProtocolVersion.HTTP_2.alpnId)) {
            return HttpProtocolVersion.HTTP_2.alpnId;
        } else {
            throw new IllegalStateException("HTTP/2 only");
        }
    }

    @Override
    public ChannelConsumer getConsumer() {
        HttpChannelConsumer.Builder builder = HttpChannelConsumer.getBuilder();
        builder.setLogger(logger)
               .setHolder(this)
               .setAuthHandler(getAuthenticationHandler())
               .setHttp2handler(this::registerHttp2Handler);
        return builder.build();
    }

    void registerHttp2Handler(Channel ch) {
        logger.debug("Starting new Channel {}", ch);
        ch.pipeline().addLast(resolver);
        ch.pipeline().addLast("gRPCDispatcher", dispatcher);
        ch.closeFuture().addListener(f -> activeChannels.remove(ch));
        activeChannels.add(ch);
    }

    public void publish(GrpcStreamHandler<?, ?> handler, Stream<Map<String, Object>> content) {
        Principal p = handler.getCurrentContext().channel().attr(PRINCIPALATTRIBUTE).get();
        BuildableConnectionContext<InetSocketAddress> cctx = getConnectionContext(handler.getCurrentContext());
        if (p != null) {
            cctx.setPrincipal(p);
        }
        content.forEach(m -> {
            Event ev = mapToEvent(cctx, m);
            if (ev != null) {
                ev.putMeta("gRPCMethod", handler.getQualifiedMethodName());
                ev.putMeta("url_path", handler.getRequestHeaders().path().toString());
                Http2Headers headers =  handler.getRequestHeaders();
                if (headers.contains("User-Agent")) {
                    ev.putMeta("user_agent", headers.get("User-Agent").toString());
                }
                ev.putMeta("host_header", headers.authority().toString());

                send(ev);
            }
        });
    }

    @Override
    public void close() {
        for (Channel c: Set.copyOf(activeChannels)) {
            c.pipeline().fireUserEventTriggered(GrpcService.CLOSE_EVENT);
            logger.trace("Closing active channel {}", c);
            c.close();
        }
        super.close();
    }

    @Override
    public String getReceiverName() {
        return "gRPCReceiver";
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "gRPC/" + getListen() + "/" + getPort();
    }

}
