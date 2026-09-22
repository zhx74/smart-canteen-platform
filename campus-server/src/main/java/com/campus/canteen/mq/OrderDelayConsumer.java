package com.campus.canteen.mq;

import com.campus.canteen.config.RabbitMQConfig;
import com.campus.canteen.entity.Orders;
import com.campus.canteen.mapper.OrderMapper;
import com.campus.canteen.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 订单延时队列消费者 - 处理超时未支付订单
 */
@Component
@Slf4j
public class OrderDelayConsumer {

    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private OrderService orderService;

    /**
     * 监听延时队列，处理超时订单
     *
     * @param orderId 订单ID
     */
    @RabbitListener(queues = RabbitMQConfig.ORDER_DELAY_QUEUE)
    public void handleOrderTimeout(Long orderId) {
        log.info("收到订单超时消息，订单ID: {}", orderId);

        try {
            Orders order = orderMapper.getById(orderId);

            if (order == null) {
                log.warn("订单不存在，订单ID: {}", orderId);
                return;
            }

            if (!Orders.PENDING_PAYMENT.equals(order.getStatus())) {
                log.info("订单状态已变更（可能已支付），无需处理，订单ID: {}, 当前状态: {}",
                        orderId, order.getStatus());
                return;
            }

            // 取消 + 归还库存。延时消息可能重复投递，条件更新保证只生效一次，不会重复归还
            Orders cancelFields = new Orders();
            cancelFields.setCancelReason("订单超时，自动取消");
            boolean cancelled = orderService.cancelOrderAndRestoreStock(orderId, cancelFields);
            log.info("超时订单处理完成，订单ID: {}, 本次是否执行取消与归还: {}", orderId, cancelled);
        } catch (Exception e) {
            // 异常不能在这里被吞掉：吞掉＝消息被 ACK、永不重投，订单状态与库存会永久不一致。
            // 也不能直接向上抛普通异常：默认 default-requeue-rejected=true 会无限重投，
            // 一条"毒消息"就能把消费者打进热循环。
            // 这里明确"拒绝且不重投"：不原地重投（那会变成毒消息热循环），
            // 消息由 order.delay.queue 的 x-dead-letter-exchange 转投到 order.delay.dlq，
            // 保留现场供人工/脚本排查与重放。业务侧不再另设定时扫盘兜底。
            log.error("超时订单处理失败，订单ID: {}，消息转入死信队列 {}", orderId, RabbitMQConfig.ORDER_DELAY_DLQ, e);
            throw new AmqpRejectAndDontRequeueException("延时消息处理失败，转入死信队列待人工处理", e);
        }
    }
}
