package com.campus.canteen.config;

import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.cache.RedisCache;
import org.springframework.data.redis.cache.RedisCacheManager;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * 把 Spring 的 {@link RedisCacheManager} 逐个包装成 {@link AntiBreakdownRedisCache}。
 *
 * <p>包装动作在 {@code getCache} 里做并缓存实例，保证同一个 cacheName 只被包一次。
 */
public class AntiBreakdownCacheManager implements CacheManager {

    private final RedisCacheManager delegate;
    private final RedissonClient redissonClient;
    private final ExecutorService rebuildExecutor;
    private final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<>();

    public AntiBreakdownCacheManager(RedisCacheManager delegate,
                                     RedissonClient redissonClient,
                                     ExecutorService rebuildExecutor) {
        this.delegate = delegate;
        this.redissonClient = redissonClient;
        this.rebuildExecutor = rebuildExecutor;
    }

    @Override
    public Cache getCache(String name) {
        return cacheMap.computeIfAbsent(name, n -> {
            Cache cache = delegate.getCache(n);
            if (cache instanceof RedisCache redisCache) {
                return new AntiBreakdownRedisCache(redisCache, redissonClient, rebuildExecutor);
            }
            return cache;
        });
    }

    @Override
    public Collection<String> getCacheNames() {
        return delegate.getCacheNames();
    }
}
