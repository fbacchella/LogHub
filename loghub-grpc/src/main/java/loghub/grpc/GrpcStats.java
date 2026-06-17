package loghub.grpc;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;
import loghub.metrics.Stats;

public class GrpcStats {

    private final Object statsHolder;

    public GrpcStats(Object statsHolder) {
        this.statsHolder = statsHolder;
    }

    public void stats(Long startTime, int httpStatus, GrpcStatus grpcStatus) {
        if (statsHolder != null) {
            long duration = startTime != null ? System.nanoTime() - startTime : 0;
            Stats.getWebMetric(statsHolder, httpStatus).update(duration, TimeUnit.NANOSECONDS);
            Stats.getMetric(statsHolder, "gRPC." + grpcStatus.resolveKey(), Timer.class).update(duration, TimeUnit.NANOSECONDS);
        }
    }

    public void received(ByteBuf currentMessage) {
        if (statsHolder != null) {
            Stats.getMetric(statsHolder, "gRPCMessage", Histogram.class).update(currentMessage.readableBytes());
        }
    }

}
