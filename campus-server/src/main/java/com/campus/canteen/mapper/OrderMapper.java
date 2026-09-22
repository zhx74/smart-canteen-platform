package com.campus.canteen.mapper;

import com.github.pagehelper.Page;
import com.campus.canteen.dto.GoodsSalesDTO;
import com.campus.canteen.dto.OrdersPageQueryDTO;
import com.campus.canteen.entity.Orders;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Mapper
public interface OrderMapper {

    // 插入订单�?
    void insert(Orders orders);

    /**
     * 分页条件查询并按下单时间排序
     * @param ordersPageQueryDTO
     */
    Page<Orders> pageQuery(OrdersPageQueryDTO ordersPageQueryDTO);

    /**
     * 根据id查询订单
     * @param id
     */
    @Select("select * from orders where id=#{id}")
    Orders getById(Long id);

    void update(Orders orders);

    /**
     * 把订单标记为已取消（条件更新，status = 6 表示已取消）。
     * 限定 status in (1,2,3,4)（待付款/待接单/已接单/派送中），即只有"进行中"的订单才能被取消：
     * - 返回 1 表示本次真正完成了取消，调用方据此归还库存
     * - 返回 0 表示订单已完成、已取消，或已被其他线程/重复投递的消息处理过，此时不归还库存
     * 既避免重复归还导致库存虚增，也避免把已完成订单改成已取消。
     *
     * @param id 订单id
     * @return 影响行数
     */
    @Update("update orders set status = 6, cancel_time = now() " +
            "where id = #{id} and status in (1, 2, 3, 4)")
    int cancelIfNotCancelled(@Param("id") Long id);

    /**
     * 根据状态统计订单数�?
     * @param status
     */
    @Select("select count(id) from orders where status = #{status}")
    Integer countStatus(Integer status);

    // 根据订单状态和下单时间查询订单
    @Select("select * from orders where status = #{status} and order_time < #{orderTime}")
    List<Orders> getByStatusAndOrderTimeLT(Integer status, LocalDateTime orderTime);


    //
    /**
     * 根据订单号查询订�?
     * @param orderNumber
     */
    @Select("select * from orders where number = #{orderNumber}")
    Orders getByNumber(String orderNumber);

    /**
     * 修改订单信息
     * @param orders
     */
    //void update(Orders orders);


    // 替换微信支付更新数据库状态的问题
    @Update("update orders set status = #{orderStatus}, pay_status = #{orderPaidStatus}, checkout_time = #{checkOutTime} where number = #{orderNumber}")
    void updateStatus(Integer orderStatus, Integer orderPaidStatus, LocalDateTime checkOutTime, String orderNumber);

    // 根据条件来统计营业额
    Double sumByMap(Map map);

    // 根据条件来统计订单数�?
    Integer countByMap(Map map);

    // 统计top10
    List<GoodsSalesDTO> getSalesTop10(LocalDateTime begin, LocalDateTime end);
}






