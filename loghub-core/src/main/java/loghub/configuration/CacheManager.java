package loghub.configuration;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.spi.CachingProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cache2k.Cache2kBuilder;
import org.cache2k.extra.jmx.JmxSupport;
import org.cache2k.jcache.ExtendedMutableConfiguration;

public class CacheManager {

    private static final Logger logger = LogManager.getLogger();

    public enum Policy {
        ACCESSED {
            @Override
            <K,
            V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration,
                                              Duration duration) {
                return configuration;
            }
        },
        CREATED {
            @Override
            <K,
            V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration,
                                              Duration duration) {
                return configuration;
            }
        },
        ETERNAL {
            @Override
            <K,
            V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration,
                                              Duration duration) {
                configuration.eternal(true);
                return configuration;
            }
        },
        MODIFIED {
            @Override
            <K,
            V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration,
                                              Duration duration) {
                configuration.expireAfterWrite(duration.toMillis(), TimeUnit.MILLISECONDS);
                return configuration;
            }
        },
        TOUCHED {
            @Override
            <K,
            V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration,
                                              Duration duration) {
                return configuration;
            }
        };
        abstract <K, V> Cache2kBuilder<K, V> configure(Cache2kBuilder<K, V> configuration, Duration duration);
    }

    public class Builder<K, V> {
        private String name;
        private final Cache2kBuilder<K, V> builder2k;
        private final Class<K> keyType;
        private final Class<V> valueType;
        private final Set<CacheEntryListenerConfiguration<K, V>> listeners = new HashSet<>();
        private Factory<? extends CacheLoader<K, V>> factory = null;
        private boolean storeByValue = true;
        private Builder(Class<K> keyType, Class<V> valueType) {
            builder2k = Cache2kBuilder.of(keyType, valueType)
                            .permitNullValues(false)
                            .storeByReference(true)
                            .keepDataAfterExpired(false);
            this.keyType = keyType;
            this.valueType = valueType;
            JmxSupport.enable(builder2k);
        }
        public Builder<K, V> setName(String name, Object parent) {
            this.name = String.format("%s@%08x", name, Integer.toUnsignedLong(parent.hashCode()));
            return this;
        }
        public Builder<K, V> setExpiry(Policy policy) {
            policy.configure(builder2k, Duration.ZERO);
            return this;
        }
        public Builder<K, V> setExpiry(Policy policy, Duration duration) {
            policy.configure(builder2k, duration);
            return this;
        }
        public Builder<K, V> setExpiry(Policy policy, long durationAmount, TemporalUnit timeUnit) {
            policy.configure(builder2k, Duration.of(durationAmount, timeUnit));
            return this;
        }
        public Builder<K, V> setCacheSize(int cacheSize) {
            builder2k.entryCapacity(cacheSize);
            return this;
        }
        public Builder<K, V> addListenerConfiguration(
                CacheEntryListenerConfiguration<K, V> config) {
            listeners.add(config);
            return this;
        }
        public Builder<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> factory) {
            this.factory = factory;
            return this;
        }
        public Builder<K, V> storeByValue(boolean storeByValue) {
            this.storeByValue = storeByValue;
            return this;
        }
        public Cache<K, V> build() {
            synchronized (cacheManager) {
                Cache<K, V> cache = cacheManager.getCache(name, keyType, valueType);
                if (cache != null) {
                    logger.debug("Reusing cache {}", name);
                } else {
                    logger.debug("creating cache {}", name);
                    ExtendedMutableConfiguration<K, V> config = ExtendedMutableConfiguration.of(builder2k);
                    if (! listeners.isEmpty()) {
                        listeners.forEach(config::addCacheEntryListenerConfiguration);
                    }
                    if (factory != null) {
                        config.setReadThrough(true);
                        config.setCacheLoaderFactory(factory);
                    }
                    config.setStoreByValue(storeByValue);
                    cache = cacheManager.createCache(name, config);
                }
                return cache;
            }
        }
    }

    private final javax.cache.CacheManager cacheManager;

    public CacheManager(ClassLoader loader) {
        synchronized (CacheManager.class) {
            CachingProvider provider = Caching.getCachingProvider(loader);
            cacheManager = provider.getCacheManager(null, loader);
        }
    }

    public <K, V> Builder<K, V> getBuilder(Class<K> keyType, Class<V> valueType) {
        return new Builder<>(keyType, valueType);
    }

    public void close() {
        cacheManager.getCachingProvider().close(cacheManager.getClassLoader());
    }

}
