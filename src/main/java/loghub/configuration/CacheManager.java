package loghub.configuration;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.spi.CachingProvider;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.jsr107.Eh107Configuration;

public class CacheManager {

    public enum Policy {
        ACCESSED {
            @SuppressWarnings("unchecked")
            @Override
            <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration) {
                return (Factory<ExpiryPolicy<K, V>>) ExpiryPolicyBuilder.timeToIdleExpiration(duration);
            }
        },
        CREATED {
            @SuppressWarnings("unchecked")
            @Override
            <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration) {
                return (Factory<ExpiryPolicy<K, V>>) ExpiryPolicyBuilder.timeToLiveExpiration(duration);
            }
        },
        ETERNAL {
            @SuppressWarnings("unchecked")
            @Override
            <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration) {
                return (Factory<ExpiryPolicy<K, V>>) ExpiryPolicyBuilder.noExpiration();
            }
        },
        MODIFIED {
            @Override
            <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration) {
                throw new UnsupportedOperationException("Ehcache don't handle modify expiration");
            }
        },
        TOUCHED {
            @Override
            <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration) {
                throw new UnsupportedOperationException("Ehcache don't handle touched expiration");
            }
        };
        abstract <K,V> Factory<ExpiryPolicy<K,V>> getPolicy(Duration duration);
    }

    public class Builder<K, V> {
        private String name;
        private Class<K> keyType;
        private Class<V> valueType;
        private Factory<ExpiryPolicy<K,V>> policy = null;
        private int cacheSize;
        private Builder(Class<K> keyType, Class<V> ValueType) {
            this.keyType = keyType;
            this.valueType = ValueType;
        }
        public Builder<K, V> setName(String name, Object parent) {
            this.name = name + "@" + parent.hashCode();
            return this;
        }
        public Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }
        public Builder<K, V> setValueType(Class<V> ValueType) {
            this.valueType = ValueType;
            return this;
        }
        public Builder<K, V> setExpiry(Policy policy, Duration duration) {
            this.policy = policy.getPolicy(duration);
            return this;
        }
        public Builder<K, V> setExpiry(Policy policy, long durationAmount, TemporalUnit timeUnit) {
            this.policy = policy.getPolicy(Duration.of(durationAmount, timeUnit));
            return this;
        }
        public Builder<K, V> setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }
        public Cache<K, V> build() {
            Cache<K, V> cache = cacheManager.getCache(name, keyType, valueType);
            if (cache != null) {
                return cache;
            } else {
                CacheConfigurationBuilder<K, V> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType,
                                                                                                                 ResourcePoolsBuilder.heap(cacheSize));
                if (policy != null) {
                    builder.withExpiry(policy.create());
                }
                CacheConfiguration<K, V> cacheConfiguration = builder.build(); 
                Configuration<K, V> eh107config = Eh107Configuration.fromEhcacheCacheConfiguration(cacheConfiguration);
                return cacheManager.createCache(name, eh107config); 
            }
        }
    }

    private final javax.cache.CacheManager cacheManager;

    public CacheManager(Properties props) {
        CachingProvider provider = Caching.getCachingProvider(props.classloader);
        cacheManager = provider.getCacheManager();
    }

    public <K, V> Builder<K, V> getBuilder(Class<K> keyType, Class<V> ValueType) {
        return new Builder<K, V>(keyType, ValueType);
    }

}
