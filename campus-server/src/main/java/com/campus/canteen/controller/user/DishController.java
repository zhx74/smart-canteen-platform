package com.campus.canteen.controller.user;

import com.campus.canteen.constant.StatusConstant;
import com.campus.canteen.entity.Dish;
import com.campus.canteen.result.Result;
import com.campus.canteen.service.DishService;
import com.campus.canteen.vo.DishVO;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController("userDishController")
@RequestMapping("/user/dish")
@Slf4j
@Tag(name = "C�?菜品浏览接口")
public class DishController {
    @Autowired
    private DishService dishService;

    /**
     * 根据分类id查询菜品
     *
     * @param categoryId
     * @return
     */
    @GetMapping("/list")
    @Operation(summary = "根据分类id查询菜品")
    public Result<List<DishVO>> list(Long categoryId) {
        Dish dish = new Dish();
        dish.setCategoryId(categoryId);
        dish.setStatus(StatusConstant.ENABLE);

        List<DishVO> list = dishService.listWithFlavor(dish);
        return Result.success(list);
    }

    /**
     * 按名称关键词搜索在售菜品（供 AI 服务调用）
     *
     * @param keyword 菜名关键词，留空返回全部在售菜品
     * @return
     */
    @GetMapping("/search")
    @Operation(summary = "按名称关键词搜索菜品")
    public Result<List<DishVO>> search(String keyword) {
        Dish dish = new Dish();
        // 空字符串也视为不限关键词，Mapper 的 <if test="name != null"> 只挡 null
        if (keyword != null && !keyword.trim().isEmpty()) {
            dish.setName(keyword.trim());
        }
        dish.setStatus(StatusConstant.ENABLE);

        List<DishVO> list = dishService.searchByName(dish);
        return Result.success(list);
    }

}

