package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RabbitMqConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillOrderMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_RESERVATION_KEY;

/**
 * 秒杀订单服务。
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final int STATUS_INIT = 0;     // 初始状态
    private static final int STATUS_PENDING = 1;    // 待处理状态
    private static final int STATUS_SENDING = 2;    // 正在发送状态
    private static final int STATUS_SENT = 3;       // 已发送状态
    private static final int STATUS_CREATED = 4;    // 创建成功状态
    private static final int STATUS_RETRY = 5;      // 重试状态
    private static final int STATUS_COMPENSATED = 6;    // 补充状态
    private static final int STATUS_REJECTED = 7;   // 拒绝状态

    private static final int MAX_PUBLISH_RETRIES = 3;
    private static final String ORDER_ID_HEADER = "seckill-order-id";
    private static final String ORDER_LOCK_PREFIX = "lock:seckill:order:";

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> COMPENSATE_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        COMPENSATE_SCRIPT = new DefaultRedisScript<>();
        COMPENSATE_SCRIPT.setLocation(new ClassPathResource("seckill_compensate.lua"));
        COMPENSATE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RabbitTemplate rabbitTemplate;
    @Resource
    private SeckillOrderMessageMapper orderMessageMapper;
    @Resource
    private RedissonClient redissonClient;

    @PostConstruct
    private void initRabbitTemplate() {
        rabbitTemplate.setMandatory(true);

        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData == null || correlationData.getId() == null) {
                return;
            }
            Long orderId = Long.valueOf(correlationData.getId());
            if (ack) {
                boolean changed = changeStatus(orderId, STATUS_SENDING, STATUS_SENT, null);
                if (changed) {
                    markRedisReservationStatus(orderId, "SENT");
                }
                return;
            }
            handlePublishFailure(orderId, "交换机确认失败：" + cause);
        });

        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            Object orderIdHeader = message.getMessageProperties().getHeaders().get(ORDER_ID_HEADER);
            if (orderIdHeader == null) {
                log.error("订单消息路由失败且缺少订单编号，exchange={}，routingKey={}，replyText={}",
                        exchange, routingKey, replyText);
                return;
            }
            Long orderId = Long.valueOf(orderIdHeader.toString());
            handlePublishFailure(orderId, "消息无法路由到队列：" + replyText);
        });
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        if (voucherId == null || UserHolder.getUser() == null) {
            return Result.fail("请求参数错误或用户未登录");
        }

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextID("order");
        VoucherOrder voucherOrder = new VoucherOrder()
                .setId(orderId)
                .setUserId(userId)
                .setVoucherId(voucherId);

        // 先持久化 INIT 状态。即使应用在 Lua 执行后宕机，定时任务也能恢复这笔订单。
        createOrderMessage(voucherOrder);

        Long result;
        try {
            result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    String.valueOf(orderId)
            );
        } catch (Exception e) {
            rejectMessage(orderId, "Redis秒杀脚本执行异常：" + e.getMessage());
            log.error("Redis秒杀脚本执行异常，orderId={}", orderId, e);
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }

        if (result == null) {
            rejectMessage(orderId, "Redis秒杀脚本未返回结果");
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }
        if (result != 0) {
            rejectMessage(orderId, seckillFailureMessage(result.intValue()));
            return Result.fail(seckillFailureMessage(result.intValue()));
        }

        if (!changeStatus(orderId, STATUS_INIT, STATUS_PENDING, null)) {
            compensateRedisStock(voucherOrder);
            return Result.fail("订单状态保存失败，请稍后重试");
        }

        // 发送异常不会立刻丢弃订单，状态会进入 RETRY，由定时任务继续投递。
        publishOrder(voucherOrder);
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void voucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        SeckillOrderMessage message = orderMessageMapper.selectById(voucherOrder.getId());
        if (message != null && message.getStatus() == STATUS_COMPENSATED) {
            throw new IllegalStateException("订单已经补偿，不能继续创建");
        }

        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            markMessageCreated(voucherOrder.getId());
            log.warn("重复的订单消息已幂等处理，userId={}，voucherId={}", userId, voucherId);
            return;
        }

        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!stockUpdated) {
            throw new IllegalStateException("MySQL库存扣减失败");
        }

        if (!save(voucherOrder)) {
            throw new IllegalStateException("订单创建失败");
        }
        markMessageCreated(voucherOrder.getId());
    }

    @Override
    public void markOrderCreated(VoucherOrder voucherOrder) {
        markRedisReservationStatus(voucherOrder.getId(), "CREATED");
        stringRedisTemplate.expire(
                SECKILL_RESERVATION_KEY + voucherOrder.getId(), 7, TimeUnit.DAYS);
    }

    @Override
    public void compensateRedisStock(VoucherOrder voucherOrder) {
        if (!isCompleteOrder(voucherOrder)) {
            throw new IllegalArgumentException("补偿订单信息不完整");
        }

        String lockName = ORDER_LOCK_PREFIX + voucherOrder.getUserId() + ":" + voucherOrder.getVoucherId();
        RLock lock = redissonClient.getLock(lockName);
        boolean locked = false;
        try {
            locked = lock.tryLock(5, 30, TimeUnit.SECONDS);
            if (!locked) {
                throw new IllegalStateException("获取订单补偿锁失败");
            }

            SeckillOrderMessage message = orderMessageMapper.selectById(voucherOrder.getId());
            if (message != null && message.getStatus() == STATUS_COMPENSATED) {
                log.info("订单已经补偿，忽略重复请求，orderId={}", voucherOrder.getId());
                return;
            }
            if (message != null && message.getStatus() == STATUS_CREATED) {
                markOrderCreated(voucherOrder);
                return;
            }

            if (orderExists(voucherOrder)) {
                markMessageCreated(voucherOrder.getId());
                markOrderCreated(voucherOrder);
                log.info("订单已落库，无需补偿Redis，orderId={}", voucherOrder.getId());
                return;
            }

            Long result = stringRedisTemplate.execute(
                    COMPENSATE_SCRIPT,
                    Collections.emptyList(),
                    voucherOrder.getId().toString(),
                    voucherOrder.getVoucherId().toString(),
                    voucherOrder.getUserId().toString()
            );
            if (result == null) {
                throw new IllegalStateException("Redis补偿脚本未返回结果");
            }
            if (result == 2L) {
                markMessageCreated(voucherOrder.getId());
                return;
            }

            orderMessageMapper.update(null,
                    new LambdaUpdateWrapper<SeckillOrderMessage>()
                            .eq(SeckillOrderMessage::getOrderId, voucherOrder.getId())
                            .ne(SeckillOrderMessage::getStatus, STATUS_CREATED)
                            .set(SeckillOrderMessage::getStatus, STATUS_COMPENSATED)
                            .set(SeckillOrderMessage::getLastError, "Redis库存已补偿")
                            .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now()));
            log.warn("Redis秒杀库存补偿完成，orderId={}，本次是否恢复库存={}",
                    voucherOrder.getId(), result == 1L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("订单补偿被中断", e);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 恢复应用宕机或 MQ 临时不可用时卡住的消息。
     */
    @Scheduled(fixedDelay = 10000L, initialDelay = 15000L)
    public void retryPendingMessages() {
        LocalDateTime retryBefore = LocalDateTime.now().minusSeconds(10);
        List<SeckillOrderMessage> messages = orderMessageMapper.selectList(
                new LambdaQueryWrapper<SeckillOrderMessage>()
                        .in(SeckillOrderMessage::getStatus,
                                Arrays.asList(STATUS_INIT, STATUS_PENDING, STATUS_RETRY))
                        .le(SeckillOrderMessage::getUpdateTime, retryBefore)
                        .orderByAsc(SeckillOrderMessage::getUpdateTime)
                        .last("LIMIT 100"));

        for (SeckillOrderMessage message : messages) {
            try {
                recoverMessage(message);
            } catch (Exception e) {
                log.error("恢复秒杀订单消息失败，orderId={}", message.getOrderId(), e);
            }
        }

        // 发送过程中宕机会留下 SENDING 状态，超过 30 秒后重新进入重试。
        List<SeckillOrderMessage> stuckMessages = orderMessageMapper.selectList(
                new LambdaQueryWrapper<SeckillOrderMessage>()
                        .eq(SeckillOrderMessage::getStatus, STATUS_SENDING)
                        .le(SeckillOrderMessage::getUpdateTime, LocalDateTime.now().minusSeconds(30))
                        .last("LIMIT 100"));
        for (SeckillOrderMessage message : stuckMessages) {
            handlePublishFailure(message.getOrderId(), "发送确认超时");
        }
    }

    private void recoverMessage(SeckillOrderMessage message) {
        if (message.getRetryCount() != null && message.getRetryCount() >= MAX_PUBLISH_RETRIES) {
            compensateRedisStock(toVoucherOrder(message));
            return;
        }

        if (message.getStatus() == STATUS_INIT) {
            Object reservationStatus = stringRedisTemplate.opsForHash()
                    .get(SECKILL_RESERVATION_KEY + message.getOrderId(), "status");
            if (reservationStatus == null) {
                rejectMessage(message.getOrderId(), "应用中断前未完成Redis预扣库存");
                return;
            }
            if (!changeStatus(message.getOrderId(), STATUS_INIT, STATUS_PENDING, null)) {
                return;
            }
        }
        publishOrder(toVoucherOrder(message));
    }

    private void publishOrder(VoucherOrder voucherOrder) {
        if (!claimForSending(voucherOrder.getId())) {
            return;
        }

        CorrelationData correlationData = new CorrelationData(voucherOrder.getId().toString());
        try {
            rabbitTemplate.convertAndSend(
                    RabbitMqConfig.ORDER_EXCHANGE,
                    RabbitMqConfig.ORDER_ROUTING_KEY,
                    voucherOrder,
                    message -> {
                        message.getMessageProperties().setHeader(ORDER_ID_HEADER, voucherOrder.getId());
                        return message;
                    },
                    correlationData
            );
        } catch (Exception e) {
            handlePublishFailure(voucherOrder.getId(), "发送消息异常：" + e.getMessage());
            log.error("订单消息发送异常，orderId={}", voucherOrder.getId(), e);
        }
    }

    private void handlePublishFailure(Long orderId, String reason) {
        int updated = orderMessageMapper.update(null,
                new LambdaUpdateWrapper<SeckillOrderMessage>()
                        .eq(SeckillOrderMessage::getOrderId, orderId)
                        .eq(SeckillOrderMessage::getStatus, STATUS_SENDING)
                        .set(SeckillOrderMessage::getStatus, STATUS_RETRY)
                        .setSql("retry_count = retry_count + 1")
                        .set(SeckillOrderMessage::getLastError, abbreviate(reason))
                        .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now()));
        if (updated == 0) {
            return;
        }

        SeckillOrderMessage message = orderMessageMapper.selectById(orderId);
        if (message != null && message.getRetryCount() >= MAX_PUBLISH_RETRIES) {
            compensateRedisStock(toVoucherOrder(message));
        }
    }

    private boolean claimForSending(Long orderId) {
        return orderMessageMapper.update(null,
                new LambdaUpdateWrapper<SeckillOrderMessage>()
                        .eq(SeckillOrderMessage::getOrderId, orderId)
                        .in(SeckillOrderMessage::getStatus, Arrays.asList(STATUS_PENDING, STATUS_RETRY))
                        .set(SeckillOrderMessage::getStatus, STATUS_SENDING)
                        .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now())) > 0;
    }

    private boolean changeStatus(Long orderId, int from, int to, String error) {
        LambdaUpdateWrapper<SeckillOrderMessage> update = new LambdaUpdateWrapper<SeckillOrderMessage>()
                .eq(SeckillOrderMessage::getOrderId, orderId)
                .eq(SeckillOrderMessage::getStatus, from)
                .set(SeckillOrderMessage::getStatus, to)
                .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now());
        if (error != null) {
            update.set(SeckillOrderMessage::getLastError, abbreviate(error));
        }
        return orderMessageMapper.update(null, update) > 0;
    }

    private void createOrderMessage(VoucherOrder voucherOrder) {
        SeckillOrderMessage message = new SeckillOrderMessage();
        message.setOrderId(voucherOrder.getId());
        message.setUserId(voucherOrder.getUserId());
        message.setVoucherId(voucherOrder.getVoucherId());
        message.setStatus(STATUS_INIT);
        message.setRetryCount(0);
        orderMessageMapper.insert(message);
    }

    private void rejectMessage(Long orderId, String reason) {
        orderMessageMapper.update(null,
                new LambdaUpdateWrapper<SeckillOrderMessage>()
                        .eq(SeckillOrderMessage::getOrderId, orderId)
                        .eq(SeckillOrderMessage::getStatus, STATUS_INIT)
                        .set(SeckillOrderMessage::getStatus, STATUS_REJECTED)
                        .set(SeckillOrderMessage::getLastError, abbreviate(reason))
                        .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now()));
    }

    private void markMessageCreated(Long orderId) {
        orderMessageMapper.update(null,
                new LambdaUpdateWrapper<SeckillOrderMessage>()
                        .eq(SeckillOrderMessage::getOrderId, orderId)
                        .ne(SeckillOrderMessage::getStatus, STATUS_COMPENSATED)
                        .set(SeckillOrderMessage::getStatus, STATUS_CREATED)
                        .set(SeckillOrderMessage::getLastError, null)
                        .set(SeckillOrderMessage::getUpdateTime, LocalDateTime.now()));
    }

    private void markRedisReservationStatus(Long orderId, String status) {
        stringRedisTemplate.opsForHash().put(SECKILL_RESERVATION_KEY + orderId, "status", status);
    }

    private boolean orderExists(VoucherOrder voucherOrder) {
        if (getById(voucherOrder.getId()) != null) {
            return true;
        }
        return query()
                .eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count() > 0;
    }

    private boolean isCompleteOrder(VoucherOrder voucherOrder) {
        return voucherOrder != null && voucherOrder.getId() != null
                && voucherOrder.getUserId() != null && voucherOrder.getVoucherId() != null;
    }

    private VoucherOrder toVoucherOrder(SeckillOrderMessage message) {
        return new VoucherOrder()
                .setId(message.getOrderId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId());
    }

    private String seckillFailureMessage(int code) {
        switch (code) {
            case 1:
                return "库存不足";
            case 2:
                return "用户已购买过";
            case 3:
                return "秒杀尚未开始";
            case 4:
                return "秒杀已经结束";
            case 5:
                return "秒杀券配置不存在";
            default:
                return "秒杀失败，请稍后重试";
        }
    }

    private String abbreviate(String value) {
        if (value == null || value.length() <= 500) {
            return value;
        }
        return value.substring(0, 500);
    }
}
