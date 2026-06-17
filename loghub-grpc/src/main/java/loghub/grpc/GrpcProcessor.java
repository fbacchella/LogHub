package loghub.grpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import loghub.AbstractBuilder;
import loghub.Helpers;
import lombok.Setter;

public abstract class GrpcProcessor<S, I, O> {

    public abstract static class Builder<B extends GrpcProcessor<S, I, O>, S, I, O> extends AbstractBuilder<B> {
    }

    @Setter
    protected S server;
    protected final Logger logger;

    protected GrpcProcessor(Builder<? extends GrpcProcessor<S, I, O>, S, I, O> builder) {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
    }

    public abstract String getServiceName();
    public abstract BinaryCodec getProtobufCodec();
    public abstract GrpcService<I, O> getHandler(GrpcStreamHandler<I, O> handler, String qualifiedMethodName, ChannelHandlerContext ctx);

}
