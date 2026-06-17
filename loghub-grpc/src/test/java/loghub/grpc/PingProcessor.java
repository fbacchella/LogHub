package loghub.grpc;

import java.io.IOException;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.netty.channel.ChannelHandlerContext;
import loghub.BuilderClass;
import loghub.ServerContext;
import loghub.grpc.Ping.PingRequest;
import loghub.grpc.Ping.PingResponse;

@BuilderClass(PingProcessor.Builder.class)
public class PingProcessor extends GrpcProcessor<ServerContext, PingRequest, PingResponse> {

    public static class Builder extends GrpcProcessor.Builder<PingProcessor, ServerContext, PingRequest, PingResponse> {
        @Override
        public PingProcessor build() {
            return new PingProcessor(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Ping ping;

    public PingProcessor(Builder builder) {
        super(builder);
        try {
            ping = new Ping();
        } catch (DescriptorValidationException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String getServiceName() {
        return "ping.PingService";
    }

    @Override
    public BinaryCodec getProtobufCodec() {
        return ping;
    }

    @Override
    public GrpcService<PingRequest, PingResponse> getHandler(
            GrpcStreamHandler<PingRequest, PingResponse> handler,
            String qualifiedMethodName,
            ChannelHandlerContext ctx
    ) {
        if ("ping.PingService.Ping".equals(qualifiedMethodName)) {
            return GrpcService.of(ping, qualifiedMethodName, i -> pingHandler(handler, i));
        } else {
            return null;
        }
    }

    private PingResponse pingHandler(GrpcStreamHandler<?, ?> handler, PingRequest request) {
        return new PingResponse(request.message(), 15L);
    }

}
