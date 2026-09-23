package com.campus.canteen.config;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.redis.cache.RedisCache;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 防击穿 / 防穿透 / 防雪崩 的 Redis 缓存包装。
 *
 * <h3>击穿：逻辑过期（主）+ SETNX 互斥（只用于冷启动）</h3>
 * 正常路径下缓存里始终有值（物理 TTL 远长于逻辑 TTL），发现逻辑过期就**立刻返回旧值**，
 * 同时交给 {@code rebuildExecutor} 里一个线程异步重建 —— 等待者零阻塞。
 * 只有在「缓存里什么都没有」的冷启动场景才需要 SETNX 互斥 + 退避重试。
 *
 * <h3>穿透：逻辑过期包装天然支持缓存 null</h3>
 * {@link LogicalExpireWrapper#getData()} 为 null 就是「查不到」这个事实被缓存了，
 * 不需要额外的空值占位类。
 *
 * <h3>雪崩：物理 TTL 长 + ±10% 随机抖动</h3>
 *
 * <h3>锁的正确性</h3>
 * 锁 value 存「实例 ID + 线程 ID」作为持有者 token，释放时用 Lua 做 compare-and-delete：
 * **值还是自己的才删**。否则当回源耗时超过锁 TTL（5 秒）时，锁已过期被别人抢走，
 * 自己回填完却把**别人的锁**删掉，互斥直接失效。
 *
 * <p>⚠️ 本类的互斥逻辑只在 {@link #get(Object, Callable)} 路径生效，
 * 因此**所有 {@code @Cacheable} 都必须配 {@code sync = true}**（这样 Spring 才会走
 * {@code Cache#get(Object, Callable)} 而不是先 {@code get(Object)} 再 {@code put(Object,Object)}）。
 */
@Slf4j
public class AntiBreakdownRedisCache implements Cache {

    private final RedisCache delegate;
    private final RedissonClient redissonClient;
    private final ExecutorService rebuildExecutor;

    /** 逻辑过期时长：30 分钟。物理 TTL 见 RedisConfiguration（12 小时） */
    private static final long LOGICAL_TTL_SECONDS = 30 * 60;

    // ---- 冷启动时的 SETNX 互斥 ----
    private static final long LOCK_TTL_SECONDS = 5;
    /** 自旋总时长上限 */
    private static final long SPIN_DEADLINE_MS = 2000;
    /** 指数退避起点 */
    private static final long SPIN_BASE_MS = 10;
    /** 单次退避上限 */
    private static final long SPIN_MAX_INTERVAL_MS = 100;

    // ---- 雪崩：物理 TTL 随机偏移比例（±10%） ----
    private static final double TTL_JITTER_RATIO = 0.1;

    /** 本 JVM 实例标识，与线程 ID 一起构成锁持有者 token */
    private static final String INSTANCE_ID = UUID.randomUUID().toString();

    /** CAS 释放锁：值还是自己的才删 */
    private static final String RELEASE_LOCK_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    /** 正在后台重建的 key，保证同一 key 只有一个重建任务 */
    private final Set<Object> rebuilding = ConcurrentHashMap.newKeySet();

    /** RedisCache#createCacheKey，构造时解析一次（原来每次 put 都反射查找，开销白给） */
    private final Method createCacheKeyMethod;

    public AntiBreakdownRedisCache(RedisCache delegate,
                                   RedissonClient redissonClient,
                                   ExecutorService rebuildExecutor) {
        this.delegate = delegate;
        this.redissonClient = redissonClient;
        this.rebuildExecutor = rebuildExecutor;
        this.createCacheKeyMethod = resolveCreateCacheKeyMethod();
    }

    private static Method resolveCreateCacheKeyMethod() {
        try {
            Method method = RedisCache.class.getDeclaredMethod("createCacheKey", Object.class);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            log.warn("RedisCache#createCacheKey 不可访问，TTL 抖动将使用兜底 key 格式", e);
            return null;
        }
    }

    private String lockKey(Object key) {
        return "cache:lock:" + getName() + ":" + key;
    }

    // ==================== 读 ====================

    @Override
    public ValueWrapper get(Object key) {
        ValueWrapper value = delegate.get(key);
        return (value != null) ? new SimpleValueWrapper(extract(value.get())) : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Class<T> type) {
        ValueWrapper value = delegate.get(key);
        if (value == null) {
            return null;
        }
        Object cached = extract(value.get());
        if (type != null && cached != null && !type.isInstance(cached)) {
            throw new IllegalStateException(
                    "Cached value is not of required type [" + type.getName() + "]: " + cached);
        }
        return (T) cached;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        LogicalExpireWrapper wrapper = readWrapper(key);

        // ① 命中且未逻辑过期 —— 热路径
        if (wrapper != null && !wrapper.logicallyExpired()) {
            return (T) wrapper.getData();
        }

        // ② 命中但已逻辑过期 —— 立刻返回旧值，只让一个线程异步重建，等待者零阻塞
        if (wrapper != null) {
            rebuildAsync(key, valueLoader);
            return (T) wrapper.getData();
        }

        // ③ 缓存里什么都没有（冷启动）—— 这时只能互斥 + 回源
        return loadWithLock(key, valueLoader);
    }

    /** 冷启动路径：SETNX 互斥，抢不到的退避重试 */
    private <T> T loadWithLock(Object key, Callable<T> valueLoader) {
        String token = INSTANCE_ID + ":" + Thread.currentThread().getId();
        RBucket<String> bucket = redissonClient.getBucket(lockKey(key));

        boolean acquired;
        try {
            acquired = Boolean.TRUE.equals(
                    bucket.setIfAbsent(token, Duration.ofSeconds(LOCK_TTL_SECONDS)));
        } catch (Exception e) {
            // Redis 不可用：直接回源，不要为了抢不到锁白等 2 秒
            log.warn("SETNX 失败，降级为直接回源。cache={} key={}", getName(), key, e);
            return call(key, valueLoader);
        }

        if (acquired) {
            try {
                // 双检：等锁期间别人可能已经回填
                LogicalExpireWrapper wrapper = readWrapper(key);
                if (wrapper != null) {
                    return (T) wrapper.getData();
                }
                T loaded = call(key, valueLoader);
                putInternal(key, loaded);
                return loaded;
            } finally {
                releaseLock(bucket, token);
            }
        }

        // 抢不到 → 指数退避重试
        LogicalExpireWrapper wrapper = spinWait(key);
        if (wrapper != null) {
            return (T) wrapper.getData();
        }

        // 退避超时还没等到 —— 自己回源，别把请求挂死
        return call(key, valueLoader);
    }

    // ==================== 写 ====================

    @Override
    public void put(Object key, Object value) {
        putInternal(key, value);
    }

    private void putInternal(Object key, Object value) {
        // 逻辑过期：存的是「业务值 + 逻辑过期时间戳」，不是裸值
        delegate.put(key, new LogicalExpireWrapper(
                value, System.currentTimeMillis() + LOGICAL_TTL_SECONDS * 1000));

        // 雪崩：给物理 TTL 加 ±10% 抖动
        Duration ttl = computeTtl();
        if (ttl != null) {
            String redisKey = resolveRedisKey(key);
            try {
                redissonClient.getBucket(redisKey).expire(ttl);
            } catch (Exception e) {
                log.debug("TTL randomization failed for key={}", redisKey, e);
            }
        }
    }

    @Override
    public void evict(Object key) {
        // 只删缓存，不再顺手删锁 —— 删别人的锁本身就是 bug，锁有自己的 TTL 会自然过期
        delegate.evict(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    // ==================== 委托 ====================

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public Object getNativeCache() {
        return delegate.getNativeCache();
    }

    // ==================== 内部 ====================

    /**
     * 读取包装值。兼容「裸值」旧格式：把它当成永不过期的数据返回，
     * 这样即使上线时忘了清 Redis，也不会因为 ClassCastException 直接 500。
     */
    private LogicalExpireWrapper readWrapper(Object key) {
        ValueWrapper value = delegate.get(key);
        if (value == null) {
            return null;
        }
        Object cached = value.get();
        if (cached instanceof LogicalExpireWrapper wrapper) {
            return wrapper;
        }
        return new LogicalExpireWrapper(cached, Long.MAX_VALUE);
    }

    /** 从包装值中取出业务数据（裸值旧格式原样返回） */
    private Object extract(Object cached) {
        return (cached instanceof LogicalExpireWrapper wrapper) ? wrapper.getData() : cached;
    }

    /** 逻辑过期：异步重建，同一 key 只放一个任务进去 */
    private void rebuildAsync(Object key, Callable<?> valueLoader) {
        if (!rebuilding.add(key)) {
            return;
        }

        // 双检（与 loadWithLock 里的双检同一个道理）：
        // 调用方是「先读到旧快照 → 再调到这里」的，两步之间有一段间隙。高并发下线程可能
        // 在间隙里被调度挤开几百毫秒，等它到达这里时，前一个重建任务**已经写完并把自己
        // 从 rebuilding 里摘掉了** —— 不重新确认一次的话，这批「拿着旧快照的迟到请求」
        // 会各自再触发一次重建（实测 200 并发下出现 2 次回源，而不是 1 次）。
        // 重建任务里是先 putInternal 再 remove，所以只要 add 成功就说明「前一个重建已完成」
        // 时，重新读一定拿到新值，从而实现退让。
        LogicalExpireWrapper latest = readWrapper(key);
        if (latest == null || !latest.logicallyExpired()) {
            rebuilding.remove(key);
            return;
        }

        try {
            rebuildExecutor.execute(() -> {
                try {
                    // 重建期间如果缓存被 @CacheEvict 清了，就放弃这次写回，免得把旧值盖回去
                    if (readWrapper(key) == null) {
                        log.debug("重建期间缓存已被清除，跳过写回。cache={} key={}", getName(), key);
                        return;
                    }
                    Object fresh = valueLoader.call();
                    putInternal(key, fresh);
                    log.debug("逻辑过期重建完成。cache={} key={}", getName(), key);
                } catch (Exception e) {
                    log.warn("逻辑过期重建失败。cache={} key={}", getName(), key, e);
                } finally {
                    rebuilding.remove(key);
                }
            });
        } catch (RejectedExecutionException e) {
            rebuilding.remove(key);
            log.warn("重建任务被拒绝。cache={} key={}", getName(), key);
        }
    }

    private <T> T call(Object key, Callable<T> valueLoader) {
        try {
            return valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    /** 退避重试：指数退避 + ±30% 抖动，避免整批等待者同时醒来打 Redis */
    private LogicalExpireWrapper spinWait(Object key) {
        long deadline = System.currentTimeMillis() + SPIN_DEADLINE_MS;
        long interval = SPIN_BASE_MS;
        while (System.currentTimeMillis() < deadline) {
            long jittered = (long) (interval * (0.7 + ThreadLocalRandom.current().nextDouble() * 0.6));
            try {
                Thread.sleep(Math.max(1, jittered));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            LogicalExpireWrapper wrapper = readWrapper(key);
            if (wrapper != null) {
                return wrapper;
            }
            interval = Math.min(interval * 2, SPIN_MAX_INTERVAL_MS);
        }
        log.warn("自旋等待耗尽，本次直接回源。cache={} key={}", getName(), key);
        return null;
    }

    /** CAS 释放：值还是自己的才删，避免误删别人的锁 */
    private void releaseLock(RBucket<String> bucket, String token) {
        try {
            redissonClient.getScript(StringCodec.INSTANCE).eval(
                    RScript.Mode.READ_WRITE,
                    RELEASE_LOCK_LUA,
                    RScript.ReturnType.INTEGER,
                    Collections.singletonList(bucket.getName()),
                    token);
        } catch (Exception e) {
            log.debug("释放锁失败（可能已自然过期）。key={}", bucket.getName(), e);
        }
    }

    /** 雪崩：base TTL ± 10% 随机偏移 */
    private Duration computeTtl() {
        Duration base = delegate.getCacheConfiguration().getTtl();
        if (base == null || base.isZero()) {
            return null;
        }
        long baseSec = base.getSeconds();
        long jitter = (long) (baseSec * TTL_JITTER_RATIO);
        long randomized = baseSec + ThreadLocalRandom.current().nextLong(-jitter, jitter + 1);
        return Duration.ofSeconds(Math.max(1, randomized));
    }

    /** 反射拿实际 Redis key（Method 已在构造时缓存） */
    private String resolveRedisKey(Object key) {
        if (createCacheKeyMethod == null) {
            return delegate.getName() + "::" + key;
        }
        try {
            return (String) createCacheKeyMethod.invoke(delegate, key);
        } catch (Exception e) {
            log.debug("反射解析 Redis key 失败，使用兜底格式", e);
            return delegate.getName() + "::" + key;
        }
    }
}
