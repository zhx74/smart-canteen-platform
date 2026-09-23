package com.campus.canteen.config;

/**
 * 逻辑过期包装值。
 *
 * <p>缓存里放的不是业务对象本身，而是「业务对象 + 逻辑过期时间戳」。key 的**物理 TTL 设得很长**
 * （见 {@link RedisConfiguration} 的 entryTtl，12 小时），所以 key 永远不会同时物理失效 ——
 * 也就不存在「热点 key 过期瞬间大量请求打到 DB」这件事，这是缓存击穿的治本解法。
 *
 * <p>过期判断完全在应用层做：发现逻辑过期后**立刻返回旧值**，同时只让一个线程去异步重建。
 * 所有等待者零阻塞，代价是秒级脏数据（餐饮菜单场景完全可接受）。
 *
 * <p>顺带解决了两个问题：
 * <ul>
 *   <li><b>穿透</b>：{@code data == null} 也能被缓存下来（那是"查不到"这个事实本身），
 *       不需要额外的空值占位类；</li>
 *   <li><b>雪崩</b>：物理 TTL 统一很长 + 下面还会加随机抖动，key 不会扎堆物理过期。</li>
 * </ul>
 *
 * <p>⚠️ <b>这个类不能加 final</b>：缓存用的是 {@code GenericJackson2JsonRedisSerializer}
 * 配 {@code DefaultTyping.NON_FINAL}，只有**非 final** 类型才会写入 {@code @class} 类型标记；
 * 一旦加了 final，Jackson 只写字段不写类型，反序列化时就还原不回本类（实测会抛
 * {@code SerializationException}）。
 */
public class LogicalExpireWrapper {

    /** 业务数据；为 null 表示「查不到」也被缓存了 */
    private Object data;

    /** 逻辑过期时间戳（epoch millis） */
    private long expireAt;

    public LogicalExpireWrapper() {
    }

    public LogicalExpireWrapper(Object data, long expireAt) {
        this.data = data;
        this.expireAt = expireAt;
    }

    /**
     * 是否已逻辑过期。
     * 方法名不用 {@code isExpired}，避免被 Jackson 当成一个额外的布尔属性写进 JSON。
     */
    public boolean logicallyExpired() {
        return System.currentTimeMillis() > expireAt;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public void setExpireAt(long expireAt) {
        this.expireAt = expireAt;
    }
}
