package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 秒杀订单可靠消息，用于记录消息从创建到落库的处理状态。
 */
@Data
@TableName("tb_seckill_order_message")
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "order_id", type = IdType.INPUT)
    private Long orderId;
    private Long userId;
    private Long voucherId;
    private Integer status;
    private Integer retryCount;
    private String lastError;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
