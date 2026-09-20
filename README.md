# 智慧食堂平台 · smart-canteen-platform

苍穹外卖风格的校园食堂点餐系统后端。Spring Boot 3.2.5 多模块架构，覆盖订单、菜品、套餐、员工管理等完整业务链路，并针对**高并发下单**做了限流、分布式锁、缓存防击穿、消息队列削峰等工程化处理。

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
- [已知限制](#已知限制)

---

## 技术栈

| 分类 | 技术 | 说明 |
|---|---|---|
| 语言 / 框架 | Java 17 · Spring Boot 3.2.5 | 多模块 Maven 工程 |
| 持久层 | MyBatis 3.0.3 · Druid 1.2.20 · PageHelper 2.1.0 | XML 映射 + 连接池 + 物理分页 |
| 数据库 | MySQL 8+ | 11 张业务表 |
| 缓存 | Spring Cache · Redis · Redisson | 自研防击穿 CacheManager |
| 消息队列 | RabbitMQ（`x-delayed-message`） | 订单超时自动取消 |
| 分布式锁 | Redisson MultiLock | 按单品粒度加锁 |
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
  │      └─► Redisson MultiLock（下单防重复）
  ▼
Mapper (MyBatis XML)
  │
  ▼
MySQL
```

## 工程化亮点

### 1. 下单防重复：Redisson MultiLock 按单品粒度加锁

普通做法是给「用户」或「全局」加一把大锁，会拖垮并发。本项目改成**按购物车里的每个菜品/套餐分别加锁**，再组合成 MultiLock：

```java
List<String> lockKeys = shoppingCartList.stream()
        .map(cart -> cart.getDishId() != null
                ? "order:dish:" + cart.getDishId()
                : "order:setmeal:" + cart.getSetmealId())
        .distinct()
        .toList();

RLock[] locks = lockKeys.stream().map(redissonClient::getLock).toArray(RLock[]::new);
RLock multiLock = redissonClient.getMultiLock(locks);
multiLock.lock();
try {
    // 校验库存 → 创建订单 → 写订单明细 → 清空购物车
} finally {
    multiLock.unlock();
}
```

**收益**：不同用户点不相关的菜，锁互不冲突；只有抢同一道菜时才串行化，并发度显著高于全局锁。

### 2. 缓存防击穿：自定义 CacheManager + 空值占位

热点 key 失效瞬间，大量请求会直接打到 DB。本项目用两个手段解决：

- **`CacheNullValue`** —— 查不到数据时写入一个「空值占位对象」而非不写缓存，避免同一不存在的 key 被反复穿透
- **`AntiBreakdownCacheManager` / `AntiBreakdownRedisCache`** —— 包装 Spring 的 `RedisCacheManager`，在缓存未命中时用 Redisson 锁保证**只有一个线程回源**，其余线程等待后读缓存

```java
Cache cache = delegate.getCache(n);
if (cache instanceof RedisCache redisCache) {
    return new AntiBreakdownRedisCache(redisCache, redissonClient);
}
```

### 3. 订单超时取消：RabbitMQ 延时队列

用 `x-delayed-message` 类型的自定义交换机实现**精确到秒的延迟投递**，下单后投递一条延时消息，到期未支付则自动取消订单。

```java
CustomExchange orderDelayedExchange() {
    args.put("x-delayed-type", "direct");
    return new CustomExchange(ORDER_DELAYED_EXCHANGE, "x-delayed-message", true, false, args);
}
```

比轮询 DB 的优势：不产生周期性全表扫描，延迟精度高，且天然削峰。

> `OrderTask.processTimeOutOrder()` 作为**兜底容灾**保留（MQ 不可用时手动开启），常规链路已由延时队列接管。

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

## 已知限制

以下几点是当前实现的边界，列出以明确后续演进方向：

- **Actuator 端点未开通** —— 已引入 `micrometer-registry-prometheus`，但缺少 `spring-boot-starter-actuator`，`/actuator/prometheus` 暂不可访问
- **索引未固化到建表脚本** —— 上述三个索引仅在测试库验证，尚未写入 `campus_canteen.sql`
- **`OrderTask.processDeliveryOrder` 仍是逐条 `update`** —— 订单量大时应改为批量更新
- **`cancelReason` 语义复用** —— 「派送中→已完成」场景也写入了 `cancelReason`，字段语义应与「取消原因」区分
- **单元测试覆盖不足** —— 目前仅 `DishServiceImplTest` 一个测试类
- **AI 模块已剥离** —— 原 `campus-ai` 模块迁移至独立仓库 [smart-canteen-ai](https://github.com/zhx74/smart-canteen-ai)

## License

MIT
