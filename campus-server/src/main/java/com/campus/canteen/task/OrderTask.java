package com.campus.canteen.task;

import com.campus.canteen.entity.Orders;
import com.campus.canteen.mapper.OrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

// 定时任务类
@Component
@Slf4j
public class OrderTask {
    @Autowired
    private OrderMapper orderMapper;

    /*
     * 超时未支付订单的取消**只走一条路径**：RabbitMQ 延时队列（order.delay.queue，
     * 见 OrderDelayConsumer）。不设定时扫盘兜底——两条路径并存会带来"谁先到谁生效"
     * 之外的额外认知成本，且条件更新本身已保证幂等，重复触发没有收益。
     */

    @Scheduled(cron = "0 0 1 * * ?")
    public void processDeliveryOrder() {
        log.info("定时处理处于派送中的订单：{}", LocalDateTime.now());

        List<Orders> ordersList = orderMapper.getByStatusAndOrderTimeLT(Orders.DELIVERY_IN_PROGRESS, LocalDateTime.now().minusHours(1));

        if (ordersList != null && !ordersList.isEmpty()) {
            for (Orders orders : ordersList) {
                orders.setStatus(Orders.COMPLETED);
                orders.setCancelReason("订单完成，自动确认");
                orders.setCancelTime(LocalDateTime.now());
                orderMapper.update(orders);
            }
        }
    }
}
