package com.campus.canteen.config;

import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableCaching
@Slf4j
public class RedisConfiguration {

    /**
     * 缓存物理 TTL 故意设得很长（12 小时），而**逻辑过期时间只有 30 分钟**
     * （见 {@link AntiBreakdownRedisCache}）。这样 key 不会「同时物理失效」，从根上消除击穿；
     * 过期判断完全落在应用层：发现逻辑过期就返回旧值 + 异步重建。
     */
    private static final Duration PHYSICAL_TTL = Duration.ofHours(12);

    /** 重建线程命名序号 */
    private static final AtomicInteger REBUILD_THREAD_SEQ = new AtomicInteger();

    @Bean
    public RedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        log.info("开始创建redis模板对象...");

        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());

        return redisTemplate;
    }

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory,
                                     RedissonClient redissonClient,
                                     @Qualifier("cacheRebuildExecutor") ExecutorService cacheRebuildExecutor) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(PHYSICAL_TTL)
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair
                                .fromSerializer(cacheSerializer())
                );
        RedisCacheManager nativeManager = RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(config)
                .build();
        return new AntiBreakdownCacheManager(nativeManager, redissonClient, cacheRebuildExecutor);
    }

    /**
     * 逻辑过期后的异步重建线程池。
     *
     * <ul>
     *   <li>core 0 / max 8：重建是低频突发，不需要常驻线程（本项目只有 3 个 cacheName，并发重建数天然有限）</li>
     *   <li>队列有界 100：防止无界堆积</li>
     *   <li><b>CallerRunsPolicy</b>：重建任务绝不能丢 —— 丢了这个 key 就永远停在过期状态，
     *       之后每次请求都会再提交一次、再丢一次，一直返回旧值。队列满时让提交者（Tomcat 线程）
     *       自己同步跑一次，天然形成背压。</li>
     *   <li>destroyMethod = shutdown：随 Spring 容器优雅关闭</li>
     * </ul>
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService cacheRebuildExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0, 8, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "cache-rebuild-" + REBUILD_THREAD_SEQ.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * 缓存专用序列化器：开启 DefaultTyping，
     * 让 JSON 中每个对象都携带 @class 类型信息，
     * 解决 List<Dish> 等泛型集合反序列化时元素类型丢失的问题。
     */
    private GenericJackson2JsonRedisSerializer cacheSerializer() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());       // 支持 LocalDateTime 等 Java 8 时间类型
        mapper.activateDefaultTyping(                       // 写入 @class 类型标记
                LaissezFaireSubTypeValidator.instance,
                ObjectMapper.DefaultTyping.NON_FINAL
        );
        return new GenericJackson2JsonRedisSerializer(mapper);
    }

    @Bean
    public RedissonClient redissonClient() throws IOException {
        log.info("开始创建RedissonClient...");
        Config config = Config.fromYAML(
            new ClassPathResource("redisson-config.yaml").getInputStream()
        );
        return Redisson.create(config);
    }
}
