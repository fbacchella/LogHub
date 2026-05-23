package loghub.netty;

import java.net.SocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.BuildableConnectionContext;
import loghub.metrics.CustomStats;
import loghub.metrics.Stats;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TRANSPORT;
import loghub.receivers.Blocking;

@Blocking
public abstract class AbstractHttpReceiver<R extends AbstractHttpReceiver<R, B>,
                                           B extends AbstractHttpReceiver.Builder<R, B>>
        extends NettyReceiver<R, Object, B>
                implements ConsumerProvider, CustomStats {

    public abstract static class Builder<R extends AbstractHttpReceiver<R, B>, B extends AbstractHttpReceiver.Builder<R, B>> extends NettyReceiver.Builder<R, Object, B> {
        protected Builder() {
            setTransport(TRANSPORT.TCP);
        }
    }

    protected final ContextExtractorGeneric<HttpRequest, ?> resolver;
    protected final ContextExtractorGeneric<Http2HeadersFrame, ?> http2resolver;

    protected AbstractHttpReceiver(B builder) {
        super(builder);
        this.resolver = new ContextExtractorGeneric<>(HttpRequest.class, this::resolveConnectionContext);
        this.http2resolver = new ContextExtractorGeneric<>(Http2HeadersFrame.class, this::resolveConnectionContext);
    }

    private <A extends SocketAddress> BuildableConnectionContext<A> resolveConnectionContext(ChannelHandlerContext ctx, Object message) {
        return (BuildableConnectionContext<A>) getTransport().getNewConnectionContext(ctx, message);
    }

    @Override
    protected void tweakNettyBuilder(B builder, NettyTransport.Builder<?, Object, ?, ?> nettyTransportBuilder) {
        super.tweakNettyBuilder(builder, nettyTransportBuilder);
        if (nettyTransportBuilder instanceof AbstractIpTransport.Builder<?, ?, ?> ipTransportBuilder) {
            ipTransportBuilder.addApplicationProtocol(ApplicationProtocolNames.HTTP_2);
            ipTransportBuilder.addApplicationProtocol(ApplicationProtocolNames.HTTP_1_1);
            HttpChannelConsumer consumer = (HttpChannelConsumer) getConsumer();
            ipTransportBuilder.setAlpnSelector(consumer.getAlpnSelector());
        }
    }

    @Override
    public ChannelConsumer getConsumer() {
        HttpChannelConsumer.Builder builder = HttpChannelConsumer.getBuilder();
        builder.setLogger(logger)
               .setHolder(this)
               .setAuthHandler(getAuthenticationHandler())
               .setModelSetup(this::configureModel);
        configureConsumer(builder);
        return builder.build();
    }

    protected void configureConsumer(HttpChannelConsumer.Builder builder) {
        // default does nothing
    }

    private void configureModel(ChannelPipeline pipeline) {
        pipeline.addBefore("HttpObjectAggregator", ContextExtractorGeneric.NAME, resolver);
        modelSetup(pipeline);
    }

    protected abstract void modelSetup(ChannelPipeline pipeline);

    @Override
    public void registerCustomStats() {
        Stats.registerHttpService(this);
    }

}
