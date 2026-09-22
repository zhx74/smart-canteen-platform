package com.campus.canteen.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.xiaoymin.knife4j.core.util.CollectionUtils;
import com.campus.canteen.constant.MessageConstant;
import com.campus.canteen.config.RabbitMQConfig;
import com.campus.canteen.context.BaseContext;
import com.campus.canteen.dto.*;
import com.campus.canteen.entity.*;
import com.campus.canteen.exception.AddressBookBusinessException;
import com.campus.canteen.exception.OrderBusinessException;
import com.campus.canteen.exception.ShoppingCartBusinessException;
import com.campus.canteen.mapper.*;
import com.campus.canteen.result.PageResult;
import com.campus.canteen.service.OrderService;
import com.campus.canteen.utils.WeChatPayUtil;
import com.campus.canteen.vo.OrderPaymentVO;
import com.campus.canteen.vo.OrderStatisticsVO;
import com.campus.canteen.vo.OrderSubmitVO;
import com.campus.canteen.vo.OrderVO;
import com.campus.canteen.websocket.WebSocketServer;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderServiceImpl implements OrderService {
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private OrderDetailMapper orderDetailMapper;
    @Autowired
    private AddressBookMapper addressBookMapper;
    @Autowired
    private ShoppingCartMapper shoppingCartMapper;
    @Autowired
    private WeChatPayUtil weChatPayUtil;
    @Autowired
    private UserMapper userMapper;
    @Autowired
    private WebSocketServer websocketServer;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private DishMapper dishMapper;
    @Autowired
    private SetmealDishMapper setmealDishMapper;

    // 用户下单
    @Override
    public OrderSubmitVO submitOrder(OrdersSubmitDTO ordersSubmitDTO) {
        Long userId = BaseContext.getCurrentId();

        // 按用户粒度加分布式锁：争抢的资源是"这个用户的购物车"，防止双击/并发重复下单。
        // 锁必须在事务外层——保证解锁前事务已提交，否则第二个线程可能在"锁已释放、数据未提交"
        // 的窗口里读到旧购物车，重复单依然防不住。
        //
        // 用 tryLock(3s) 而不是 lock()：lock() 是无等待上限的阻塞，同一用户并发时后续请求会一直挂起。
        // 一旦持锁线程卡住（慢 SQL、行锁等待），挂起的线程会占满 Tomcat 工作线程把整个服务拖垮，
        // 而且看门狗仍在续期、锁永不自动释放，这个"卡住"可能是永久的。等待上限 + 快速失败才是对的。
        //
        // 只传 waitTime、不传 leaseTime：传了 leaseTime 会关掉看门狗，
        // 业务跑超过租约时间时锁被自动释放，第二个线程即可进入临界区，互斥随即失效。
        RLock lock = redissonClient.getLock("order:submit:" + userId);
        boolean locked;
        try {
            locked = lock.tryLock(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderBusinessException(MessageConstant.ORDER_SUBMIT_TOO_FREQUENT);
        }
        if (!locked) {
            throw new OrderBusinessException(MessageConstant.ORDER_SUBMIT_TOO_FREQUENT);
        }
        try {
            // 编程式事务，让 DB 写入的提交发生在锁释放之前
            TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
            Orders order = transactionTemplate.execute(status -> doSubmitOrder(ordersSubmitDTO, userId));

            // 事务提交后再发延时消息（15 分钟后检查是否支付），避免回滚了却仍发出超时取消消息
            long delayMillis = 15 * 60 * 1000;
            rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_DELAYED_EXCHANGE,
                RabbitMQConfig.ORDER_DELAY_ROUTING_KEY,
                order.getId(),
                message -> {
                    message.getMessageProperties().setDelay((int) delayMillis);
                    return message;
                }
            );
            log.info("订单创建成功，订单ID: {}, 已发送延时消息", order.getId());

            return OrderSubmitVO.builder()
                    .id(order.getId())
                    .orderTime(order.getOrderTime())
                    .orderNumber(order.getNumber())
                    .orderAmount(order.getAmount())
                    .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 下单核心逻辑，运行在事务内。购物车查询与判空都在锁内执行，
     * 确保并发/重复提交时第二个请求读到的是已清空的购物车。
     */
    private Orders doSubmitOrder(OrdersSubmitDTO ordersSubmitDTO, Long userId) {
        // 处理各种业务异常(地址簿为空，购物车数据为空)
        AddressBook addressBook = addressBookMapper.getById(ordersSubmitDTO.getAddressBookId());
        if (addressBook == null) {
            throw new AddressBookBusinessException(MessageConstant.ADDRESS_BOOK_IS_NULL);
        }

        // 查询当前用户购物车数据
        ShoppingCart shoppingCart = new ShoppingCart();
        shoppingCart.setUserId(userId);
        List<ShoppingCart> shoppingCartList = shoppingCartMapper.list(shoppingCart);
        if (shoppingCartList == null || shoppingCartList.isEmpty()) {
            throw new ShoppingCartBusinessException(MessageConstant.SHOPPING_CART_IS_NULL);
        }

        // 先扣库存再落单：库存不足会抛异常，整个事务回滚，订单/明细/购物车都不会留下痕迹
        deductStock(shoppingCartList);

        // 创建订单实体
        Orders order = new Orders();
        BeanUtils.copyProperties(ordersSubmitDTO, order);
        order.setPhone(addressBook.getPhone());
        order.setAddress(addressBook.getDetail());
        order.setConsignee(addressBook.getConsignee());
        order.setStatus(Orders.PENDING_PAYMENT); // 待支付状态
        order.setPayStatus(Orders.UN_PAID);
        order.setOrderTime(LocalDateTime.now());
        order.setNumber(String.valueOf(System.currentTimeMillis()));
        order.setUserId(userId);

        // 插入订单
        orderMapper.insert(order);

        // 插入订单明细
        List<OrderDetail> orderDetailList = new ArrayList<>();
        for (ShoppingCart cart : shoppingCartList) {
            OrderDetail orderDetail = new OrderDetail();
            BeanUtils.copyProperties(cart, orderDetail, "id");
            orderDetail.setOrderId(order.getId());
            orderDetailList.add(orderDetail);
        }
        orderDetailMapper.insertBatch(orderDetailList);

        // 清空购物车
        shoppingCartMapper.deleteByUserId(userId);

        return order;
    }

    /**
     * 扣减库存。以菜品为维度聚合后按 dishId 升序逐条扣减：
     * - 聚合：同一道菜在购物车里可能出现多行（不同口味），套餐还要按 setmeal_dish 展开
     * - 升序：多个订单同时扣多道菜时，加锁顺序一致才不会交叉死锁
     * 判断与扣减在同一条 UPDATE 内完成，影响行数为 0 即库存不足，
     * 抛异常交由外层事务整体回滚。
     */
    private void deductStock(List<ShoppingCart> shoppingCartList) {
        Map<Long, Integer> dishQty = new TreeMap<>();
        for (ShoppingCart cart : shoppingCartList) {
            accumulateDishQty(dishQty, cart.getDishId(), cart.getSetmealId(), cart.getNumber());
        }

        for (Map.Entry<Long, Integer> entry : dishQty.entrySet()) {
            if (dishMapper.deductStock(entry.getKey(), entry.getValue()) == 0) {
                throw new OrderBusinessException(MessageConstant.STOCK_NOT_ENOUGH);
            }
        }
    }

    /**
     * 把一条购物车（或订单明细）展开累加到"菜品 → 数量"的映射中。
     * 单品直接累加；套餐按 setmeal_dish 展开成组成菜品，数量为 copies × 该行份数。
     * 购物车里可能同时存在单品和含同一道菜的套餐，必须累加到同一条目，
     * 否则同一道菜会被分两批扣减，升序加锁的防死锁效果随之失效。
     */
    private void accumulateDishQty(Map<Long, Integer> dishQty, Long dishId, Long setmealId, Integer number) {
        int qty = number == null ? 0 : number;
        if (qty <= 0) {
            return;
        }

        if (dishId != null) {
            dishQty.merge(dishId, qty, Integer::sum);
            return;
        }

        if (setmealId != null) {
            for (SetmealDish setmealDish : setmealDishMapper.getBySetmealId(setmealId)) {
                int copies = setmealDish.getCopies() == null ? 0 : setmealDish.getCopies();
                if (copies > 0) {
                    dishQty.merge(setmealDish.getDishId(), copies * qty, Integer::sum);
                }
            }
        }
    }

    /**
     * 取消订单并归还库存。用户取消、商家拒单、商家取消、超时自动取消四个入口统一走这里。
     * 用条件更新（仅 status in (1,2,3,4) 的"进行中"订单可取消）保证幂等：
     * 只有抢到本次状态变更的调用才归还库存；重复调用、已完成订单被误取消等情况影响行数为 0，直接跳过。
     */
    @Override
    public boolean cancelOrderAndRestoreStock(Long orderId, Orders cancelFields) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        return Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            if (orderMapper.cancelIfNotCancelled(orderId) == 0) {
                log.info("订单此前已取消，跳过库存归还，订单ID: {}", orderId);
                return Boolean.FALSE;
            }

            // 取消原因、拒单原因、退款状态等由各入口自行决定，一并写入
            if (cancelFields != null) {
                cancelFields.setId(orderId);
                orderMapper.update(cancelFields);
            }

            restoreStockByOrderId(orderId);
            return Boolean.TRUE;
        }));
    }

    /**
     * 按订单明细归还库存。展开规则与下单扣减完全一致（套餐按 setmeal_dish 的 copies 展开），
     * 保证"怎么扣的就怎么还"。
     */
    private void restoreStockByOrderId(Long orderId) {
        Map<Long, Integer> dishQty = new TreeMap<>();
        for (OrderDetail detail : orderDetailMapper.getByOrderId(orderId)) {
            accumulateDishQty(dishQty, detail.getDishId(), detail.getSetmealId(), detail.getNumber());
        }

        for (Map.Entry<Long, Integer> entry : dishQty.entrySet()) {
            dishMapper.restoreStock(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 订单支付
     */
    public OrderPaymentVO payment(OrdersPaymentDTO ordersPaymentDTO) throws Exception {
        Long userId = BaseContext.getCurrentId();
        User user = userMapper.getById(userId);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", "ORDERPAID");
        OrderPaymentVO vo = jsonObject.toJavaObject(OrderPaymentVO.class);
        vo.setPackageStr(jsonObject.getString("package"));

        Integer OrderPaidStatus = Orders.PAID;
        Integer OrderStatus = Orders.TO_BE_CONFIRMED;
        LocalDateTime check_out_time = LocalDateTime.now();
        String orderNumber = ordersPaymentDTO.getOrderNumber();

        log.info("调用updateStatus, 用于替换支付后更新数据库状态的问题");
        orderMapper.updateStatus(OrderStatus, OrderPaidStatus, check_out_time, orderNumber);

        // 通过websocket向客户端浏览器推送消息
        Map map = new HashMap();
        map.put("type", 1);
        Orders orderDB = orderMapper.getByNumber(ordersPaymentDTO.getOrderNumber());
        map.put("orderId", orderDB.getId());
        map.put("content", "订单号：" + ordersPaymentDTO.getOrderNumber());
        map.put("sendTime", System.currentTimeMillis());

        String json = JSONObject.toJSONString(map);
        websocketServer.sendToAllClient(json);

        return vo;
    }

    /**
     * 支付成功，修改订单状态
     */
    public void paySuccess(String outTradeNo) {
        // 发布事件
        Map<String, Object> event = Map.of("outTradeNo", outTradeNo, "event", "payment.success");
        rabbitTemplate.convertAndSend("order.exchange", "order.payment", event);
    }

    /**
     * 用户端订单分页查询
     */
    public PageResult pageQuery4User(int pageNum, int pageSize, Integer status) {
        PageHelper.startPage(pageNum, pageSize);

        OrdersPageQueryDTO ordersPageQueryDTO = new OrdersPageQueryDTO();
        ordersPageQueryDTO.setUserId(BaseContext.getCurrentId());
        ordersPageQueryDTO.setStatus(status);

        Page<Orders> page = orderMapper.pageQuery(ordersPageQueryDTO);

        List<OrderVO> list = new ArrayList();

        if (page != null && page.getTotal() > 0) {
            for (Orders orders : page) {
                Long orderId = orders.getId();
                List<OrderDetail> orderDetails = orderDetailMapper.getByOrderId(orderId);

                OrderVO orderVO = new OrderVO();
                BeanUtils.copyProperties(orders, orderVO);
                orderVO.setOrderDetailList(orderDetails);

                list.add(orderVO);
            }
        }
        return new PageResult(page.getTotal(), list);
    }

    /**
     * 查询订单详情
     */
    public OrderVO details(Long id) {
        Orders orders = orderMapper.getById(id);
        List<OrderDetail> orderDetailList = orderDetailMapper.getByOrderId(orders.getId());

        OrderVO orderVO = new OrderVO();
        BeanUtils.copyProperties(orders, orderVO);
        orderVO.setOrderDetailList(orderDetailList);

        return orderVO;
    }

    /**
     * 用户取消订单
     */
    public void userCancelById(Long id) throws Exception {
        Orders ordersDB = orderMapper.getById(id);

        if (ordersDB == null) {
            throw new OrderBusinessException(MessageConstant.ORDER_NOT_FOUND);
        }

        if (ordersDB.getStatus() > 2) {
            throw new OrderBusinessException(MessageConstant.ORDER_STATUS_ERROR);
        }

        Orders cancelFields = new Orders();
        cancelFields.setCancelReason("用户取消");
        if (ordersDB.getStatus().equals(Orders.TO_BE_CONFIRMED)) {
            cancelFields.setPayStatus(Orders.REFUND);
        }

        // 取消 + 归还库存。status 与 cancel_time 由条件更新写入，重复调用不会重复归还
        cancelOrderAndRestoreStock(id, cancelFields);
    }

    /**
     * 再来一单
     */
    public void repetition(Long id) {
        Long userId = BaseContext.getCurrentId();
        List<OrderDetail> orderDetailList = orderDetailMapper.getByOrderId(id);

        List<ShoppingCart> shoppingCartList = orderDetailList.stream().map(x -> {
            ShoppingCart shoppingCart = new ShoppingCart();
            BeanUtils.copyProperties(x, shoppingCart, "id");
            shoppingCart.setUserId(userId);
            shoppingCart.setCreateTime(LocalDateTime.now());
            return shoppingCart;
        }).collect(Collectors.toList());

        shoppingCartMapper.insertBatch(shoppingCartList);
    }

    /**
     * 订单搜索
     */
    public PageResult conditionSearch(OrdersPageQueryDTO ordersPageQueryDTO) {
        PageHelper.startPage(ordersPageQueryDTO.getPage(), ordersPageQueryDTO.getPageSize());

        Page<Orders> page = orderMapper.pageQuery(ordersPageQueryDTO);
        List<OrderVO> orderVOList = getOrderVOList(page);

        return new PageResult(page.getTotal(), orderVOList);
    }

    private List<OrderVO> getOrderVOList(Page<Orders> page) {
        List<OrderVO> orderVOList = new ArrayList<>();
        List<Orders> ordersList = page.getResult();
        
        if (!CollectionUtils.isEmpty(ordersList)) {
            for (Orders orders : ordersList) {
                OrderVO orderVO = new OrderVO();
                BeanUtils.copyProperties(orders, orderVO);
                String orderDishes = getOrderDishesStr(orders);
                orderVO.setOrderDishes(orderDishes);
                orderVOList.add(orderVO);
            }
        }
        return orderVOList;
    }

    /**
     * 根据订单id获取菜品信息字符串
     */
    private String getOrderDishesStr(Orders orders) {
        List<OrderDetail> orderDetailList = orderDetailMapper.getByOrderId(orders.getId());

        List<String> orderDishList = orderDetailList.stream().map(x -> {
            String orderDish = x.getName() + "*" + x.getNumber() + ";";
            return orderDish;
        }).collect(Collectors.toList());

        return String.join("", orderDishList);
    }

    /**
     * 各个状态的订单数量统计
     */
    public OrderStatisticsVO statistics() {
        Integer toBeConfirmed = orderMapper.countStatus(Orders.TO_BE_CONFIRMED);
        Integer confirmed = orderMapper.countStatus(Orders.CONFIRMED);
        Integer deliveryInProgress = orderMapper.countStatus(Orders.DELIVERY_IN_PROGRESS);

        OrderStatisticsVO orderStatisticsVO = new OrderStatisticsVO();
        orderStatisticsVO.setToBeConfirmed(toBeConfirmed);
        orderStatisticsVO.setConfirmed(confirmed);
        orderStatisticsVO.setDeliveryInProgress(deliveryInProgress);
        return orderStatisticsVO;
    }

    /**
     * 接单
     */
    public void confirm(OrdersConfirmDTO ordersConfirmDTO) {
        Orders orders = Orders.builder()
                .id(ordersConfirmDTO.getId())
                .status(Orders.CONFIRMED)
                .build();

        orderMapper.update(orders);
    }

    /**
     * 拒单
     */
    public void rejection(OrdersRejectionDTO ordersRejectionDTO) throws Exception {
        Orders ordersDB = orderMapper.getById(ordersRejectionDTO.getId());

        if (ordersDB == null || !ordersDB.getStatus().equals(Orders.TO_BE_CONFIRMED)) {
            throw new OrderBusinessException(MessageConstant.ORDER_STATUS_ERROR);
        }

        Integer payStatus = ordersDB.getPayStatus();
        if (payStatus == Orders.PAID) {
            log.info("申请退款");
        }

        Orders cancelFields = new Orders();
        cancelFields.setRejectionReason(ordersRejectionDTO.getRejectionReason());

        // 取消 + 归还库存（幂等）
        cancelOrderAndRestoreStock(ordersDB.getId(), cancelFields);
    }

    /**
     * 取消订单
     */
    public void cancel(OrdersCancelDTO ordersCancelDTO) throws Exception {
        Orders ordersDB = orderMapper.getById(ordersCancelDTO.getId());
        if (ordersDB == null) {
            throw new OrderBusinessException(MessageConstant.ORDER_NOT_FOUND);
        }

        // 只有"进行中"的订单才可取消。这个校验必须挡在退款之前，
        // 否则会出现"钱已退、订单却没被取消"的不一致
        Integer status = ordersDB.getStatus();
        if (status == null || status < Orders.PENDING_PAYMENT || status > Orders.DELIVERY_IN_PROGRESS) {
            throw new OrderBusinessException(MessageConstant.ORDER_STATUS_ERROR);
        }

        Integer payStatus = ordersDB.getPayStatus();
        if (payStatus == 1) {
            String refund = weChatPayUtil.refund(
                    ordersDB.getNumber(),
                    ordersDB.getNumber(),
                    new BigDecimal(0.01),
                    new BigDecimal(0.01));
            log.info("申请退款");
        }

        Orders cancelFields = new Orders();
        cancelFields.setCancelReason(ordersCancelDTO.getCancelReason());

        // 取消 + 归还库存（幂等）
        cancelOrderAndRestoreStock(ordersDB.getId(), cancelFields);
    }

    /**
     * 派送订单
     */
    public void delivery(Long id) {
        Orders ordersDB = orderMapper.getById(id);

        if (ordersDB == null || !ordersDB.getStatus().equals(Orders.CONFIRMED)) {
            throw new OrderBusinessException(MessageConstant.ORDER_STATUS_ERROR);
        }

        Orders orders = new Orders();
        orders.setId(ordersDB.getId());
        orders.setStatus(Orders.DELIVERY_IN_PROGRESS);

        orderMapper.update(orders);
    }

    /**
     * 完成订单
     */
    public void complete(Long id) {
        Orders ordersDB = orderMapper.getById(id);

        if (ordersDB == null || !ordersDB.getStatus().equals(Orders.DELIVERY_IN_PROGRESS)) {
            throw new OrderBusinessException(MessageConstant.ORDER_STATUS_ERROR);
        }

        Orders orders = new Orders();
        orders.setId(ordersDB.getId());
        orders.setStatus(Orders.COMPLETED);
        orders.setDeliveryTime(LocalDateTime.now());

        orderMapper.update(orders);
    }

    // 客户催单
    @Override
    public void reminder(Long id) {
        Orders ordersDB = orderMapper.getById(id);

        if(ordersDB == null) {
            throw new OrderBusinessException(MessageConstant.ORDER_NOT_FOUND);
        }

        Map map = new HashMap();
        map.put("type", 2);
        map.put("orders", id);
        map.put("content", "订单号：" + ordersDB.getNumber());
        map.put("sendTime", System.currentTimeMillis());

        String json = JSON.toJSONString(map);
        websocketServer.sendToAllClient(json);
    }
}
