package com.campus.canteen.utils;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class JwtUtil {

    /**
     * 仅用于预热的密钥。必须 ≥32 字节（HS256 最短要求），
     * 否则 signWith 会先抛 WeakKeyException，走不到 compact()，也就预热不到 Serializer。
     */
    private static final String WARMUP_SECRET =
            "warmup-warmup-warmup-warmup-warmup-warmup-warmup-warmup";

    /*
     * 预热 jjwt 的服务发现，规避 jjwt 0.12.3 的已知线程安全缺陷（见 jwtk/jjwt#873）。
     *
     * 问题：jjwt 的 Services.loadFirst() 内部依赖 ServiceLoader，而 ServiceLoader 的迭代器
     * 不是线程安全的 —— 它的 nextName 是共享可变字段。冷启动后第一批并发请求同时触发这次
     * 服务发现时，会出现「一个线程刚把 nextName 置上、另一个线程消费掉」的交错，导致其中
     * 一个线程拿到 java.util.NoSuchElementException。表现为：应用刚启动时约 0.5%~1% 的请求
     * 被判定未登录（401），热起来之后不再出现。
     *
     * 为什么预热能治本：jjwt 把解析到的 ServiceLoader 实例缓存在 SERVICE_CACHE 中，provider
     * 一旦解析成功就进入其内部 providers 映射，后续查找会直接从映射命中、不再触碰有竞态的
     * 那个迭代器。而类的静态初始化块由 JVM 保证单线程执行（其他线程会阻塞等待），在这里各
     * 触发一次即可把竞态窗口提前关闭 —— 即便首次请求本身就是并发的，也依然安全。
     *
     * 为什么生成侧也要预热：compact() 走的是同一套 ServiceLoader 查找（上游 issue 的原始堆栈
     * 就在 DefaultJwtBuilder.compact），并发登录同样会撞。
     */
    static {
        try {
            createJWT(WARMUP_SECRET, 1000L, Map.<String, Object>of("warmup", 1));
        } catch (Exception ignored) {
            // 仅用于预热，失败可接受：真正调用时的异常由上层自行处理
        }
        try {
            parseJWT(WARMUP_SECRET, "warmup");
        } catch (Exception ignored) {
            // 同上。这里的解析必然失败（不是合法 token），目的只是触发 deserializer 的服务发现
        }
    }

    private static SecretKey getKey(String secretKey) {
        return new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    public static String createJWT(String secretKey, long ttlMillis, Map<String, Object> claims) {
        long expMillis = System.currentTimeMillis() + ttlMillis;
        return Jwts.builder()
                .claims(claims)
                .expiration(new Date(expMillis))
                .signWith(getKey(secretKey))
                .compact();
    }

    public static Claims parseJWT(String secretKey, String token) {
        return Jwts.parser()
                .verifyWith(getKey(secretKey))
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
