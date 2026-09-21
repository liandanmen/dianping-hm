package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RabbitMqConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    private static final String SECKILL_ORDER_KEY = "seckill:order:";

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RabbitTemplate rabbitTemplate;

    private final Map<String, VoucherOrder> pendingOrders = new ConcurrentHashMap<>();

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @PostConstruct
    private void initRabbitTemplate() {
        rabbitTemplate.setMandatory(true);

        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData == null) {
                return;
            }
            String orderId = correlationData.getId();
            VoucherOrder voucherOrder = pendingOrders.remove(orderId);
            if (ack) {
                return;
            }
            log.error("订单消息发送到交换机失败，orderId={}，原因：{}", orderId, cause);
            compensateRedisStock(voucherOrder);
        });

        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            Object body = rabbitTemplate.getMessageConverter().fromMessage(message);
            if (!(body instanceof VoucherOrder)) {
                log.error("订单消息路由失败，但消息体解析失败，exchange={}，routingKey={}，replyText={}",
                        exchange, routingKey, replyText);
                return;
            }
            VoucherOrder voucherOrder = (VoucherOrder) body;
            pendingOrders.remove(String.valueOf(voucherOrder.getId()));
            log.error("订单消息路由到队列失败，orderId={}，exchange={}，routingKey={}，replyText={}",
                    voucherOrder.getId(), exchange, routingKey, replyText);
            compensateRedisStock(voucherOrder);
        });
    }

    // 秒杀业务
    @Override
    public Result seckillVoucher(Long voucherId) {
        String userId = UserHolder.getUser().getId().toString();

        Long execute = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId
        );

        int value = execute.intValue();
        if (value != 0) {
            return Result.fail(value == 1 ? "库存不足" : "用户已购买过");
        }

        long orderId = redisIdWorker.nextID("order");

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(Long.valueOf(userId));
        voucherOrder.setVoucherId(voucherId);

        CorrelationData correlationData = new CorrelationData(String.valueOf(orderId));
        pendingOrders.put(String.valueOf(orderId), voucherOrder);
        try {
            rabbitTemplate.convertAndSend(
                    RabbitMqConfig.ORDER_EXCHANGE,
                    RabbitMqConfig.ORDER_ROUTING_KEY,
                    voucherOrder,
                    correlationData
            );
        } catch (Exception e) {
            pendingOrders.remove(String.valueOf(orderId));
            compensateRedisStock(voucherOrder);
            log.error("订单消息发送异常，orderId={}", orderId, e);
            return Result.fail("订单创建失败，请稍后重试");
        }

        log.info("订单消息发送成功，订单编号：{}", orderId);
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void voucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            log.warn("用户已购买过该优惠券，userId={}，voucherId={}", userId, voucherId);
            return;
        }

        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            throw new RuntimeException("MySQL库存扣减失败");
        }

        save(voucherOrder);
    }

    @Override
    public void compensateRedisStock(VoucherOrder voucherOrder) {
        if (voucherOrder == null) {
            return;
        }

        Long orderId = voucherOrder.getId();
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        if (orderId == null || userId == null || voucherId == null) {
            log.error("订单补偿失败，订单信息不完整：{}", voucherOrder);
            return;
        }

        int count = query()
                .eq("id", orderId)
                .or()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            log.info("订单已落库，无需补偿Redis，orderId={}", orderId);
            return;
        }

        stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.opsForSet().remove(SECKILL_ORDER_KEY + voucherId, userId.toString());
        log.warn("已补偿Redis秒杀库存，orderId={}，userId={}，voucherId={}", orderId, userId, voucherId);
    }
}
