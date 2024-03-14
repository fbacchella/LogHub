package loghub.processors;

import java.time.Duration;

import javax.cache.Cache;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.proxy.ProxyManager;
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

    @Setter
    public static class Builder extends Processor.Builder<RateLimiter> {
        private Expression key = null;
        private long rate;
        private long burstRate = -1;
        private CacheManager cacheManager = new CacheManager(getClass().getClassLoader());
        public RateLimiter build() {
            return new RateLimiter(this);
        }
    }
    public static RateLimiter.Builder getBuilder() {
        return new RateLimiter.Builder();
    }

    private final Expression keyExpression;
    private final Bucket singleBucket;
    private final ProxyManager<Object> buckets;
    private final BucketConfiguration configuration;

    public RateLimiter(Builder builder) {
        keyExpression = builder.key;
        if (builder.burstRate < builder.rate && builder.burstRate > 0) {
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
        if (keyExpression != null) {
            configuration = BucketConfiguration.builder()
                                               .addLimit(limit)
                                               .build();
            Cache<Object, byte[]> cache = builder.cacheManager
                                                 .getBuilder(Object.class, byte[].class)
                                                 .setName("RateLimiter", this)
                                                 .build();
            buckets = new JCacheProxyManager<>(cache);
            singleBucket = null;
        } else {
            singleBucket = Bucket.builder()
                                 .withSynchronizationStrategy(SynchronizationStrategy.LOCK_FREE)
                                 .addLimit(limit)
                                 .build();
            buckets = null;
            configuration = null;
        }
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Bucket currentBucket;
        if (singleBucket != null) {
            currentBucket = singleBucket;
        } else {
            currentBucket = buckets.builder().build(keyExpression.eval(event), () -> configuration);
        }
        try {
            currentBucket.asBlocking().consume(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

}
