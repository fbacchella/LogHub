package loghub.netty;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.ssl.ApplicationProtocolNames;
import loghub.metrics.CustomStats;
import loghub.metrics.Stats;
import loghub.netty.transport.AbstractIpTransport;
import loghub.netty.transport.NettyTransport;
import loghub.netty.transport.TRANSPORT;
import loghub.receivers.Blocking;

@Blocking
public abstract class AbstractHttpReceiver<R extends AbstractHttpReceiver<R, B>, B
                extends AbstractHttpReceiver.Builder<R, B>> extends NettyReceiver<R, HttpMessage, B>
                implements ConsumerProvider, CustomStats {

    public abstract static class Builder<R extends AbstractHttpReceiver<R, B>, B extends AbstractHttpReceiver.Builder<R, B>> extends NettyReceiver.Builder<R, HttpMessage, B> {
        protected Builder() {
            setTransport(TRANSPORT.TCP);
        }
    }

    protected final ContextExtractor<R, HttpMessage, B> resolver;

    protected AbstractHttpReceiver(B builder) {
        super(builder);
        this.resolver = new ContextExtractor<>(HttpMessage.class, this);
    }

    @Override
    protected void tweakNettyBuilder(B builder, NettyTransport.Builder<?, HttpMessage, ?, ?> nettyTransportBuilder) {
        super.tweakNettyBuilder(builder, nettyTransportBuilder);
        if (isWithSSL() && nettyTransportBuilder instanceof AbstractIpTransport.Builder<?, ?, ?> ipTransportBuilder) {
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
        pipeline.addBefore("HttpObjectAggregator", ContextExtractor.NAME, resolver);
        modelSetup(pipeline);
    }

    protected abstract void modelSetup(ChannelPipeline pipeline);

    @Override
    public void registerCustomStats() {
        Stats.registerHttpService(this);
    }

}
