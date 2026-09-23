# 智慧食堂平台 · smart-canteen-platform

苍穹外卖风格的校园食堂点餐系统后端。Spring Boot 3.2.5 多模块架构，覆盖订单、菜品、套餐、员工管理等完整业务链路，并针对**高并发下单**做了限流、分布式锁、DB 条件更新防超卖、缓存防击穿、消息队列削峰等工程化处理。

> 配套 AI 模块在独立仓库：[smart-canteen-ai](https://github.com/zhx74/smart-canteen-ai)
> （Python + LangChain Agent + RAG 知识库 + FastAPI SSE，通过 HTTP 调用本服务的业务接口）

## 目录

- [技术栈](#技术栈)
- [架构设计](#架构设计)
- [工程化亮点](#工程化亮点)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [接口文档](#接口文档)
- [性能验证](#性能验证)
- [运行监控](#运行监控)
- [已知限制](#已知限制)

---

## 技术栈

| 分类 | 技术 | 说明 |
|---|---|---|
| 语言 / 框架 | Java 17 · Spring Boot 3.2.5 | 多模块 Maven 工程 |
| 持久层 | MyBatis 3.0.3 · Druid 1.2.20 · PageHelper 2.1.0 | XML 映射 + 连接池 + 物理分页 |
| 数据库 | MySQL 8+ | 11 张业务表 |
| 缓存 | Spring Cache · Redis · Redisson | 自研防击穿 CacheManager（逻辑过期 + SETNX 互斥） |
| 消息队列 | RabbitMQ（`x-delayed-message`） | 订单超时自动取消 |
| 分布式锁 | Redisson RLock | 按用户粒度加锁（防重复下单） |
| 限流熔断 | Resilience4j | 下单限流 + 支付熔断 |
| 实时通信 | WebSocket | 订单状态推送 |
| 任务调度 | Spring Task | 派送中订单自动确认 |
| 认证 | JWT (jjwt 0.12.3) | 管理端 / 用户端双令牌 |
| 接口文档 | Knife4j 4.5.0 (OpenAPI 3) | 分组展示管理端 / 用户端 |
| 可观测性 | Micrometer Tracing (Brave) · Prometheus | 链路追踪 + 指标导出 |
| 文件存储 | 阿里云 OSS 3.10.2 | 菜品图片上传 |
| 工具库 | Lombok · FastAPI 风格统一响应 · AspectJ | AOP 自动填充公共字段 |

## 架构设计

```
smart-canteen-platform
├── campus-common     公共模块：常量、异常、工具类、第三方配置属性
├── campus-pojo       数据对象：DTO / Entity / VO
└── campus-server     核心业务：controller / service / mapper / config / mq / task
```

分层的意义：`campus-common` 与 `campus-pojo` **零业务依赖**，可被任何模块复用；`campus-server` 承载全部业务逻辑与基础设施配置。

### 请求链路

```
Client
  │
  ▼
JwtTokenAdminInterceptor / JwtTokenUserInterceptor   ← 令牌校验，写入 BaseContext(ThreadLocal)
  │
  ▼
Controller (@RateLimiter / @CircuitBreaker)          ← Resilience4j 限流熔断
  │
  ▼
Service  ──► Redis 缓存（AntiBreakdownCacheManager 防击穿）
  │      └─► Redisson RLock order:submit:{userId}（防重复下单）
  ▼
Mapper (MyBatis XML)
  │
  ▼
MySQL
```

## 工程化亮点

### 1. 并发下单：锁防重复下单 + DB 条件更新防超卖

这是**两个不同的问题，用两套不同机制**，不能混为一谈。

**① 防重复下单 —— Redisson 按用户粒度加锁**

```java
RLock lock = redissonClient.getLock("order:submit:" + userId);   // 用户粒度
boolean locked = lock.tryLock(3, TimeUnit.SECONDS);              // 不传 leaseTime，保住看门狗续期
// ... 业务在 TransactionTemplate 事务内执行
lock.unlock();                                                   // 锁在事务外层：先提交、再解锁
```

争抢的资源就是「这个用户的购物车」，所以按**用户**粒度锁既够用、又不会让不同用户互相干扰。

**② 防超卖 —— 不用分布式锁，交给 DB 的条件更新**

```sql
UPDATE dish SET stock = stock - #{number}
WHERE id = #{dishId} AND stock >= #{number}
```

判断**影响行数**：`1` = 扣减成功，`0` = 库存不足 → 抛业务异常，由外层事务整体回滚。
「判断 + 扣减」在同一条 SQL 内完成，InnoDB 的当前读会对该行加排他锁，天然原子。

为什么这里不用锁：按用户粒度锁锁不住「别人买同一道菜」；按菜品粒度锁则要先查购物车才知道该锁哪些菜（锁形同虚设），而且热销菜会把订单全部串行化。**用分布式锁是拿更粗、更贵的锁，去做数据库已经免费做好的事。**

> **A/B 对照实验**：唯一变量是去掉那个「影响行数判断」—— 订单数立刻从 **10 变成 100**（库存已为 0 却下出 100 单），而扣减次数仍是 10。
> 证明防超卖靠的是「**条件更新 + 影响行数判断 + 事务回滚**」三件套；光写 `WHERE stock >= n` 只保证库存不被扣成负数。

### 2. 缓存防击穿：逻辑过期 + 异步单线程重建

热点 key 失效瞬间，大量请求会直接打到 DB。常见做法是「互斥锁 + 自旋等待」，但自旋总有上限：回源一旦慢于上限，等待者集体下探，**保护恰好在最慢的时刻失效**。

本项目改用**逻辑过期**：缓存里存的不是业务对象本身，而是「业务值 + 逻辑过期时间戳」（`LogicalExpireWrapper`），**物理 TTL 设成 12 小时**（远长于 30 分钟的逻辑 TTL）—— key 永远不会「同时物理失效」，从根上不存在击穿窗口。

```java
LogicalExpireWrapper wrapper = readWrapper(key);
if (wrapper != null && !wrapper.logicallyExpired()) {
    return (T) wrapper.getData();          // ① 热路径：直接命中
}
if (wrapper != null) {                     // ② 已逻辑过期
    rebuildAsync(key, valueLoader);        //    只放一个线程异步重建（见下）
    return (T) wrapper.getData();          //    其余请求立刻拿到旧值，零阻塞
}
return loadWithLock(key, valueLoader);     // ③ 冷启动：SETNX 互斥 + 指数退避重试
```

效果对比：

| | 互斥 + 自旋 | 逻辑过期 |
|---|---|---|
| 过期后首个请求 | 回源，耗时 = 回源耗时 | 立刻返回旧值 |
| 其余请求 | 自旋等最多 2s，超时后集体回源 | **全部立刻返回** |
| 最坏回源并发 | = 并发数（自旋超时后） | **恒为 1** |

**异步重建的去重窗口（实测踩出来的坑）**：光靠 `ConcurrentHashMap.add()` 抢名额是不够的。`get()` 里「读到过期快照」和「调用 `rebuildAsync`」之间有一段间隙，而重建任务是在**写回之后**才把 key 从 `rebuilding` 里移除的 —— 高并发下线程会在这段间隙被调度挤开几百毫秒，等它到达 `add()` 时前一个重建已经完成并摘掉了自己，于是**又触发一次回源**（实测 200 并发回了 **2** 次源）。修法是拿到名额后再 `readWrapper` **双检**一次，确认仍然过期才真的重建，否则退让（与冷启动路径的双检同一思路）。

**实测数据**（`dishWithFlavors::1`，241 个菜品；`listWithFlavor` 是 N+1 查询 ⇒ 单次重建就是 243 条 SELECT）：

| 并发 | 成功 / 失败 | 回源 SELECT | 折算重建次数 |
|---|---|---|---|
| 200 | 200 / 0 | 243 | 1.00 |
| 500 | 500 / 0 | 243 | 1.00 |
| 1000 | 1000 / 0 | 243 | 1.00 |

并发从 200 涨到 1000，回源量**恒定 243 条**；若保护失效，1000 并发应为 243,000 条 —— 实测是它的 **0.1%**，且没有任何请求落在 2000ms 的自旋上限上（逻辑过期路径零阻塞）。修复前 200 并发是 485 条（2.00 × 243）。

重建线程池参数：core 0 / max 8 / 有界队列 100 / **`CallerRunsPolicy`** —— 重建任务绝不能丢：丢了这个 key 会永远停在过期状态，之后每次请求都再提交一次、再丢一次，一直返回旧值；队列满时让提交者自己跑，天然形成背压。

**锁的持有者校验**：冷启动锁的 value 存「实例 ID + 线程 ID」，释放时用 Lua 做 compare-and-delete —— **值还是自己的才删**。否则回源耗时超过锁 TTL（5 秒）时，锁已过期被别人抢走，自己回填完却把**别人的锁**删掉，互斥直接失效。

注意锁用的是 **SETNX 原语**（`RBucket.setIfAbsent`），**不是 Redisson 的 `RLock`** —— Redisson 在本项目只作为 Redis 客户端使用。

另外两兄弟：

- **防雪崩**：物理 TTL 12 小时 + **±10% 随机抖动**，key 不会扎堆过期
- **防穿透**：`LogicalExpireWrapper.data == null` 就是「查不到」这个事实被缓存了，天然支持缓存 null，不需要任何空值占位类

> ⚠️ **实现上的关键细节**：`LogicalExpireWrapper` **不能声明成 `final`**。缓存用 `GenericJackson2JsonRedisSerializer` + `DefaultTyping.NON_FINAL`，只有**非 final** 类型才会写 `@class` 类型标记；加了 final 就写不进去、反序列化还原不回原类型（实测直接抛 `SerializationException`）。

> 配套约束：所有 `@Cacheable` 必须配 `sync = true`，Spring 才会走 `Cache#get(Object, Callable)` 这条路径 —— 这样锁的获取与释放都在**同一个方法调用栈**内，不需要跨 `get()`/`put()` 传递锁持有者。

### 3. 订单超时取消：RabbitMQ 延时队列

用 `x-delayed-message` 类型的自定义交换机实现**精确到秒的延迟投递**，下单后投递一条延时消息，到期未支付则自动取消订单。

```java
CustomExchange orderDelayedExchange() {
    args.put("x-delayed-type", "direct");
    return new CustomExchange(ORDER_DELAYED_EXCHANGE, "x-delayed-message", true, false, args);
}
```

比轮询 DB 的优势：不产生周期性全表扫描，延迟精度高，且天然削峰。

> 超时取消**只走延时队列这一条路径**，不做定时扫盘兜底；消费异常的消息经死信交换机（DLX）转投 `order.delay.dlq`，便于事后排查与重放。

### 4. 限流与熔断：Resilience4j

```java
@PostMapping("/submit")
@RateLimiter(name = "orderSubmit", fallbackMethod = "fallback")
public Result<OrderSubmitVO> submitOrder(@RequestBody OrdersSubmitDTO dto) { ... }

@PutMapping("/payment")
@CircuitBreaker(name = "paymentService", fallbackMethod = "paymentFallback")
public Result<OrderPaymentVO> payment(@RequestBody OrdersPaymentDTO dto) { ... }
```

| 组件 | 实例 | 参数 |
|---|---|---|
| RateLimiter | `orderSubmit` | 500 次/秒，超时立即拒绝（`timeoutDuration: 0`） |
| CircuitBreaker | `paymentService` | 失败率 >50% 熔断，10 秒后进入半开 |

两者都配了 `fallbackMethod`，降级时返回友好提示而非 500。

### 5. AOP 自动填充公共字段

`@AutoFill` 注解 + `AutoFillAspect`，在 INSERT / UPDATE 时自动写入 `create_time`、`update_time`、`create_user`、`update_user`，业务代码不再关心这些字段。操作人 ID 从 `BaseContext`（ThreadLocal）取。

## 快速开始

### 前置依赖

| 服务 | 版本 | 默认端口 |
|---|---|---|
| JDK | 17+ | — |
| MySQL | 8.0+ | 3306 |
| Redis | 6+ | 6379 |
| RabbitMQ | 3.8+（**需装延时消息插件**） | 5672 |
| PostgreSQL | 仅 AI 模块需要 | 5432 |

### 1. 初始化数据库

```bash
mysql -uroot -p < campus_canteen.sql
```

### 2. 配置环境变量

```bash
cp campus-server/src/main/resources/application.yml.example \
   campus-server/src/main/resources/application.yml
```

`application.yml` 已在 `.gitignore` 中（含 JWT 密钥），需自行创建。密码类配置统一从环境变量读取：

| 变量 | 说明 | 默认值 |
|---|---|---|
| `DB_HOST` / `DB_PORT` / `DB_NAME` | MySQL 地址 | `localhost` / `3306` / `campus_canteen` |
| `DB_USERNAME` / `DB_PASSWORD` | MySQL 账号 | `root` / — |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | Redis 地址 | `localhost` / `6379` / — |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | MQ 地址 | `localhost` / `5672` / `guest` / `guest` |
| `JWT_ADMIN_SECRET` / `JWT_USER_SECRET` | JWT 签名密钥（≥32 字节） | 开发占位值 |
| `ALIOSS_ENDPOINT` / `ALIOSS_ACCESS_KEY_ID` / `ALIOSS_ACCESS_KEY_SECRET` / `ALIOSS_BUCKET_NAME` | 阿里云 OSS | — |
| `WECHAT_APPID` / `WECHAT_SECRET` | 微信登录 | — |

生成密钥：`openssl rand -base64 48`

### 3. 安装 RabbitMQ 延时消息插件

```bash
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

### 4. 启动

```bash
mvn clean package -DskipTests
java -jar campus-server/target/campus-server-1.0-SNAPSHOT.jar
```

或在 IDE 中运行 `CampusApplication`，默认端口 **8080**。

### Docker 方式

```bash
docker compose up -d
```

## 项目结构

```
campus-server/src/main/java/com/campus/canteen/
├── annotation/      @AutoFill 等自定义注解
├── aspect/          AutoFillAspect —— INSERT/UPDATE 公共字段自动填充
├── config/          Redis / RabbitMQ / OSS / WebSocket / WebMvc 配置
│                    AntiBreakdown*  防击穿缓存实现
├── controller/
│   ├── admin/       管理端：员工、分类、菜品、套餐、统计报表
│   ├── user/        用户端：登录、下单、购物车、地址簿
│   └── notigy/      通知（WebSocket 相关）
├── handler/         全局异常处理、SQL 异常转换
├── interceptor/     JWT 令牌校验（管理端 / 用户端）
├── mapper/          MyBatis Mapper 接口
├── mq/              OrderDelayConsumer —— 延时消息消费者
├── service/         业务接口与实现
├── task/            OrderTask / WebSocketTask 定时任务
└── websocket/       WebSocketServer 订单状态推送
```

数据表（11 张）：`employee` `category` `dish` `dish_flavor` `setmeal` `setmeal_dish` `user` `address_book` `shopping_cart` `orders` `order_detail`

## 接口文档

启动后访问 Knife4j：

```
http://localhost:8080/doc.html
```

已按管理端（`/admin/**`）与用户端（`/user/**`）分组。核心接口：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/user/user/login/phone` | 手机号登录，返回 JWT |
| POST | `/user/order/submit` | 提交订单（限流） |
| PUT | `/user/order/payment` | 订单支付（熔断） |
| GET | `/user/order/historyOrders` | 历史订单分页 |
| PUT | `/user/order/cancel/{id}` | 取消订单 |
| POST | `/user/order/repetition/{id}` | 再来一单 |
| GET | `/user/order/reminder/{id}` | 客户催单 |

## 性能验证

针对订单表做过完整的慢 SQL 定位与索引优化，测试环境灌入 **102.4 万行**订单数据 + 20 万行订单明细（表数据 158.7MB，`innodb_buffer_pool_size` 仅 128MB），用 `EXPLAIN ANALYZE` 采集真实执行耗时。

| 场景 | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| 查用户最近 10 单<br>`WHERE user_id=? ORDER BY order_time DESC LIMIT 10` | 951 ms<br>（全表扫 + filesort） | **0.072 ms**<br>（`ref` + Backward index scan） | ≈ 13000× |
| 销量 Top10 关联查询<br>`order_detail JOIN orders` + 时间范围 | 506 ms<br>（`Using temporary`） | **22.3 ms**<br>（驱动表换为 orders） | ≈ 23× |

建立的索引：

```sql
ALTER TABLE orders       ADD INDEX idx_user_time   (user_id, order_time);
ALTER TABLE orders       ADD INDEX idx_status_time (status, order_time);
ALTER TABLE order_detail ADD INDEX idx_order_id    (order_id);
```

> 索引列顺序遵循最左前缀：等值条件列在前（定位），排序/范围列在后（借有序性消除 filesort）。

**一个反直觉的发现**：`SELECT * FROM orders WHERE status=1 AND order_time < ?` 这条 SQL，**加了 `idx_status_time` 后优化器依然选择全表扫描**（988ms），强制走索引反而慢到 **6379ms**。原因是 `status` 基数只有 7，单值命中 14.6 万行（14.3%），且 `SELECT *` 不被索引覆盖 → 每行都要回表随机读，代价高于顺序全表扫描。改成覆盖查询 `SELECT COUNT(*)` 后降至 **33.4ms**。

> **结论**：索引不是加了就一定快，低基数等值列 + 非覆盖 `SELECT *` 时回表随机 I/O 会让索引成为负优化。

## 运行监控

接入 Spring Boot Actuator，暴露 4 个运维端点（`/actuator/**`）：

| 端点 | 内容 |
|---|---|
| `/actuator/health` | 应用与中间件整体健康状态，`{"status":"UP"}` |
| `/actuator/info` | 应用元信息 |
| `/actuator/metrics` | **76 个指标**的查询入口，支持 `/metrics/{name}` 取单项 |
| `/actuator/prometheus` | 全部指标的 Prometheus 文本格式（316 行），可直接对接 Prometheus + Grafana |

实测可读的关键指标：

| 指标 | 用途 |
|---|---|
| `jvm.memory.used` | 堆 / 非堆各内存池占用（排查内存泄漏、OOM 的主指标） |
| `jvm.gc.pause` | GC 停顿次数 `COUNT` / 累计 `TOTAL_TIME` / 单次最大 `MAX` |
| `http.server.requests` | 按 URI 维度的请求耗时，可取 P95 / P99 |
| `resilience4j.circuitbreaker.*` | 熔断器状态、失败率、慢调用率 |
| `resilience4j.ratelimiter.*` | 限流器可用令牌数、等待线程数 |
| `rabbitmq.*` | 连接数、发布 / 消费 / 拒绝消息数 |
| `tomcat.sessions.*` | 会话数、拒绝数 |

**安全约束**：仅暴露上表 4 个端点，`env` / `beans` / `heapdump` 这类会泄露配置、Bean 结构与内存快照的敏感端点一律不开；`health` 详情设为 `when-authorized`，避免对外暴露中间件地址。

> 注意：`micrometer-registry-prometheus` 只负责**把指标转成 Prometheus 格式**，真正暴露 HTTP 端点的是 `spring-boot-starter-actuator`。两者缺一，`/actuator/prometheus` 都是 404。

## 已知限制

以下几点是当前实现的边界，列出以明确后续演进方向：

- **索引未固化到建表脚本** —— 上述三个索引仅在测试库验证，尚未写入 `campus_canteen.sql`
- **`OrderTask.processDeliveryOrder` 仍是逐条 `update`** —— 订单量大时应改为批量更新
- **`cancelReason` 语义复用** —— 「派送中→已完成」场景也写入了 `cancelReason`，字段语义应与「取消原因」区分
- **单元测试覆盖不足** —— 目前仅 `DishServiceImplTest` 一个测试类
- **AI 模块已剥离** —— 原 `campus-ai` 模块迁移至独立仓库 [smart-canteen-ai](https://github.com/zhx74/smart-canteen-ai)
- **逻辑过期会带来秒级脏数据** —— 逻辑过期后先返回旧值、再异步重建，这个窗口内读到的是旧数据（餐饮菜单场景可接受）。写路径仍走 `@CacheEvict` 即时失效，不依赖逻辑过期，因此「改完立刻可见」不受影响
- **重建去重是进程内的** —— `rebuilding` 是 `ConcurrentHashMap`，多实例部署时每个实例各会发起一次重建（当前单实例，暂不构成问题）
- **冷启动仍有 2 秒自旋上限** —— 只在「缓存里完全没有值」时才会走到；已改为指数退避（10ms 起、单次上限 100ms、±30% 抖动），平均等待从约 50ms 降到约 10ms

## License

MIT
