package com.campus.canteen.service.impl;

import com.alibaba.fastjson.JSON;
import com.campus.canteen.entity.Orders;
import com.campus.canteen.mapper.OrderMapper;
import com.campus.canteen.websocket.WebSocketServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 订单 MQ 消费者。
 * 注：原先监听 order.create.queue 的 processOrderCreation 已移除——
 * 下单改为在 OrderServiceImpl 中同步完成，该队列没有任何生产者，方法永远不会被触发，
 * 其中按菜品粒度的 multiLock 也与实际争抢的资源（用户购物车）不匹配，留着会造成误导。
 */
@Component
@Slf4j
public class OrderConsumer {
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private WebSocketServer websocketServer;

    @RabbitListener(queues = "order.payment.queue")
    public void processPaymentSuccess(Map<String, Object> event) {
        String outTradeNo = (String) event.get("outTradeNo");
        Orders ordersDB = orderMapper.getByNumber(outTradeNo);

        Orders orders = Orders.builder()
                .id(ordersDB.getId())
                .status(Orders.TO_BE_CONFIRMED)
                .payStatus(Orders.PAID)
                .checkoutTime(LocalDateTime.now())
                .build();

        orderMapper.update(orders);

        // WebSocket推送
        Map<String, Object> push = Map.of("type", 1, "orderId", ordersDB.getId());
        websocketServer.sendToAllClient(JSON.toJSONString(push));
    }
}
