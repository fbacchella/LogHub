package loghub.processors;

import java.time.Duration;

import javax.cache.Cache;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BlockingBucket;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.distributed.proxy.RemoteBucketBuilder;
import io.github.bucket4j.grid.jcache.JCacheProxyManager;
import io.github.bucket4j.local.SynchronizationStrategy;
import loghub.BuilderClass;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.CacheManager;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(RateLimiter.Builder.class)
public class RateLimiter extends Processor {

    private interface RateLimiting {
        boolean limit(Event ev) throws ProcessorException;
    }

    @Setter
    public static class Builder extends Processor.Builder<RateLimiter> {
        private Expression key = null;
        private long rate;
        private long burstRate = -1;
        private boolean dropping = false;
        private CacheManager cacheManager = new CacheManager(getClass().getClassLoader());
        public RateLimiter build() {
            return new RateLimiter(this);
        }
    }
    public static RateLimiter.Builder getBuilder() {
        return new RateLimiter.Builder();
    }

    private final RateLimiting consumer;

    public RateLimiter(Builder builder) {
        Bucket singleBucket;
        ProxyManager<Object> buckets;
        BucketConfiguration configuration;
        RemoteBucketBuilder<Object> bucketsBuilder;
        if (builder.burstRate > 0 && builder.burstRate < builder.rate) {
            throw new IllegalArgumentException("Burst rate must be superior to rate");
        }
        Bandwidth limit;
        if (builder.burstRate > 0) {
            limit = Bandwidth.builder()
                             .capacity(builder.burstRate)
                             .refillGreedy(builder.rate, Duration.ofSeconds(1))
                             .build();
        } else {
            limit = Bandwidth.builder()
                            .capacity(builder.rate)
                            .refillGreedy(builder.rate, Duration.ofSeconds(1))
                            .build();
        }
        if (builder.key != null) {
            configuration = BucketConfiguration.builder()
                                               .addLimit(limit)
                                               .build();
            Cache<Object, byte[]> cache = builder.cacheManager
                                                 .getBuilder(Object.class, byte[].class)
                                                 .setName("RateLimiter", this)
                                                 .build();
            buckets = new JCacheProxyManager<>(cache);
            bucketsBuilder = buckets.builder();
            singleBucket = null;
        } else {
            singleBucket = Bucket.builder()
                                 .withSynchronizationStrategy(SynchronizationStrategy.LOCK_FREE)
                                 .addLimit(limit)
                                 .build();
            configuration = null;
            bucketsBuilder = null;
        }
        int filter = (builder.dropping ? 1 : 0) | ((builder.key != null ? 1 : 0) << 1);
        consumer = switch (filter) {
        case 0 -> { // blocking and no key
            BlockingBucket bb = singleBucket.asBlocking();
            yield e -> processBlockNoKey(bb);
        }
        case 1 -> { // dropping and no key
            Drop dropper = new Drop();
            yield e -> processDropNoKey(e, singleBucket, dropper);
        }
        case 2 -> { // blocking and key
            Expression keyExpression = builder.key;
            yield e -> processBlockWithKey(e, bucketsBuilder, configuration, keyExpression);
        }
        case 3 -> { // dropping and key
            Drop dropper = new Drop();
            Expression keyExpression = builder.key;
            yield e -> processDropWithKey(e, bucketsBuilder, dropper, configuration, keyExpression);
        }
        default ->
            throw new IllegalStateException("Unreachable code");
        };
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        return consumer.limit(event);
    }

    private boolean processBlockNoKey(BlockingBucket currentBucket) {
        try {
            currentBucket.consume(1);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean processBlockWithKey(Event event, RemoteBucketBuilder<Object> bucketsBuilder, BucketConfiguration configuration, Expression keyExpression) throws ProcessorException {
        Bucket currentBucket = bucketsBuilder.build(keyExpression.eval(event), () -> configuration);
        // Don't really bother with caching asBlocking(), it return this
        return processBlockNoKey(currentBucket.asBlocking());
    }

    private boolean processDropWithKey(Event event, RemoteBucketBuilder<Object> bucketsBuilder, Drop dropper, BucketConfiguration configuration, Expression keyExpression)
            throws ProcessorException {
        Bucket currentBucket = bucketsBuilder.build(keyExpression.eval(event), () -> configuration);
        return processDropNoKey(event, currentBucket, dropper);
    }

    private boolean processDropNoKey(Event e, Bucket currentBucket, Drop dropper) {
        if (! currentBucket.tryConsume(1)) {
            e.insertProcessor(dropper);
        }
        return true;
    }

}
