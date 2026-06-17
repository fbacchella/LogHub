package loghub.grpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import loghub.BuilderClass;
import lombok.Setter;

@BuilderClass(CentreonBroker.Builder.class)
public class CentreonBroker extends GrpcReceiverProcessor<Map<String, Object>> {

    @Setter
    public static class Builder extends GrpcReceiverProcessor.Builder<CentreonBroker, Map<String, Object>> {
        private String name = "BBDO-Client-Output-LogHub";

        @Override
        public CentreonBroker build() {
            return new CentreonBroker(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final CentreonBrokerDecoder decoder;
    private final AtomicInteger messageCountAtomic = new AtomicInteger();
    private final String name;

    public CentreonBroker(Builder builder) {
        super(builder);
        this.name = builder.name;
        try {
            decoder = new CentreonBrokerDecoder();
        } catch (DescriptorValidationException | IOException e) {
            throw new IllegalStateException("Failed to initialize CentreonBroker decoder", e);
        }
    }

    @Override
    public BinaryCodec getProtobufCodec() {
        return decoder;
    }

    @Override
    public GrpcService<Map<String, Object>, Map<String, Object>> getHandler(
            GrpcStreamHandler<Map<String, Object>, Map<String, Object>> handler, String qualifiedMethodName,
            ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        return switch (qualifiedMethodName) {
            case "com.centreon.broker.stream.centreon_bbdo.exchange" ->
                    GrpcService.of(decoder, qualifiedMethodName, i -> exchange(handler, i), () -> doClose(handler, channel));
            default -> null;
        };
    }

    private Map<String, Object> exchange(GrpcStreamHandler<?, ?> handler, Map<String, Object> message) {
        if (message.containsKey("buffer")) {
            BbdoPacket inPacket = (BbdoPacket) message.get("buffer");
            BbdoPacket outPacket = switch (inPacket.event()) {
                case BbdoEvent.BBDO_WELCOME -> {
                    Map<String, Object> welcome = new HashMap<>(inPacket.payload());
                    welcome.put("peer_type", "BROKER");
                    welcome.remove("extensions");
                    welcome.put("broker_name", name);
                    yield new BbdoPacket(BbdoEvent.BBDO_WELCOME, inPacket.destinationId(), inPacket.sourceId(), welcome);
                }
                case BbdoEvent.BBDO_PB_STOP -> {
                    int toAck = messageCountAtomic.getAndSet(0);
                    yield new BbdoPacket(BbdoEvent.BBDO_PB_ACK, inPacket.destinationId(), inPacket.sourceId(), Map.of("acknowledged_events", toAck));
                }
                case BbdoEvent.STORAGE_PB_METRIC, BbdoEvent.STORAGE_PB_STATUS -> null;
                case Object o when inPacket.event().category == BbdoCategory.STORAGE -> null;
                default -> {
                    Map<String, Object> buffer = Map.of(
                            "event", inPacket.event(),
                            "destination_id", inPacket.destinationId(),
                            "source_id", inPacket.sourceId(),
                            "data", inPacket.payload()
                    );
                    message.put("buffer", buffer);
                    publish(handler, Stream.of(message));
                    yield null;
                }
            };
            if (messageCountAtomic.incrementAndGet() > 1000 && outPacket == null) {
                int toAck = messageCountAtomic.getAndSet(0);
                outPacket = new BbdoPacket(BbdoEvent.BBDO_PB_ACK,
                        inPacket.destinationId(), inPacket.sourceId(), Map.of("acknowledged_events", toAck));
            }
            if (outPacket != null) {
                Object sourceId = message.get("source_id");
                Object destinationId = message.get("destination_id");
                return Map.of("buffer", ByteString.copyFrom(outPacket.serialize(decoder)), "source_id", destinationId, "destination_id", sourceId);
            } else {
                return null;
            }
        } else {
            publish(handler, Stream.of(message));
            return null;
        }
    }

    private void doClose(GrpcStreamHandler<?, ?> handler, Channel channel) {
        int toAck = messageCountAtomic.getAndSet(0);
        BbdoPacket outPacket = new BbdoPacket(
                BbdoEvent.BBDO_PB_ACK,
                0, 0, Map.of("acknowledged_events", toAck)
        );
        Map<String, Object> lastAck = Map.of("buffer", ByteString.copyFrom(outPacket.serialize(decoder)), "source_id", 0, "destination_id", 1);
        byte[] messageBytes = decoder.encode("com.centreon.broker.stream.CentreonEvent", lastAck);
        handler.writeGrpcDataFrame(channel, messageBytes);
        handler.sendTrailers(channel, GrpcStatus.OK);
    }

    @Override
    public String getServiceName() {
        return "com.centreon.broker.stream.centreon_bbdo";
    }

}
