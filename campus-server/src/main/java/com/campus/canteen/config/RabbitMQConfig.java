package com.campus.canteen.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    // 原有配置（保留）
    @Bean
    public Exchange orderExchange() {
        return ExchangeBuilder.directExchange("order.exchange").durable(true).build();
    }

    @Bean
    public Queue orderCreateQueue() {
        return QueueBuilder.durable("order.create.queue").build();
    }

    @Bean
    public Binding orderCreateBinding() {
        return BindingBuilder.bind(orderCreateQueue()).to(orderExchange()).with("order.create").noargs();
    }

    @Bean
    public Queue orderPaymentQueue() {
        return QueueBuilder.durable("order.payment.queue").build();
    }

    @Bean
    public Binding orderPaymentBinding() {
        return BindingBuilder.bind(orderPaymentQueue()).to(orderExchange()).with("order.payment").noargs();
    }

    // ========== 新增：订单延时队列配置 ==========

    // 延时交换机名称
    public static final String ORDER_DELAYED_EXCHANGE = "order.delayed.exchange";

    // 延时队列名称
    public static final String ORDER_DELAY_QUEUE = "order.delay.queue";

    // 路由键
    public static final String ORDER_DELAY_ROUTING_KEY = "order.delay";

    // 延时队列的死信交换机 / 死信队列 / 死信路由键
    // 消费者抛 AmqpRejectAndDontRequeueException（或消息 TTL 到期、队列超长）时，消息转投到这里，
    // 而不是被静默丢弃 —— 超时取消订单只有 MQ 一条路径，失败消息必须有落点。
    public static final String ORDER_DELAY_DLX = "order.delay.dlx";
    public static final String ORDER_DELAY_DLQ = "order.delay.dlq";
    public static final String ORDER_DELAY_DLQ_ROUTING_KEY = "order.delay.dlq";

    /**
     * 创建延时交换机（使用 x-delayed-message 类型）
     */
    @Bean
    public CustomExchange orderDelayedExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(ORDER_DELAYED_EXCHANGE, "x-delayed-message", true, false, args);
    }

    /**
     * 创建延时队列。
     * 绑定死信交换机：消费失败被 reject 的消息不再原地丢弃，而是投到 order.delay.dlq。
     */
    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(ORDER_DELAY_QUEUE)
                .deadLetterExchange(ORDER_DELAY_DLX)
                .deadLetterRoutingKey(ORDER_DELAY_DLQ_ROUTING_KEY)
                .build();
    }

    /**
     * 延时队列的死信交换机（普通 direct 交换机即可，不需要 x-delayed-message）
     */
    @Bean
    public DirectExchange orderDelayDlx() {
        return ExchangeBuilder.directExchange(ORDER_DELAY_DLX).durable(true).build();
    }

    /**
     * 死信队列。这里只做"落点"，不挂消费者：
     * 消息进来说明超时取消失败，需要人工或运维脚本查看后决定重放还是手工归还库存。
     * 一旦给它挂上自动消费者，就等于又造了一条隐式重试链路，反而掩盖问题。
     */
    @Bean
    public Queue orderDelayDlq() {
        return QueueBuilder.durable(ORDER_DELAY_DLQ).build();
    }

    /**
     * 死信队列绑定到死信交换机
     */
    @Bean
    public Binding orderDelayDlqBinding(Queue orderDelayDlq, DirectExchange orderDelayDlx) {
        return BindingBuilder.bind(orderDelayDlq)
                .to(orderDelayDlx)
                .with(ORDER_DELAY_DLQ_ROUTING_KEY);
    }

    /**
     * 绑定延时队列到延时交换机
     */
    @Bean
    public Binding orderDelayBinding(Queue orderDelayQueue, CustomExchange orderDelayedExchange) {
        return BindingBuilder.bind(orderDelayQueue)
                .to(orderDelayedExchange)
                .with(ORDER_DELAY_ROUTING_KEY)
                .noargs();
    }
}
