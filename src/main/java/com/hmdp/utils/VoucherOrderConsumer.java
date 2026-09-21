package com.hmdp.utils;

import com.hmdp.config.RabbitMqConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


// 消费者类，处理优惠券订单
@Component
@Slf4j
public class VoucherOrderConsumer {
    private static final String ORDER_LOCK_PREFIX = "lock:seckill:order:";

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private RedissonClient redissonClient;

    @RabbitListener(queues = RabbitMqConfig.ORDER_QUEUE)
    public void handleVouherOrder(
            VoucherOrder voucherOrder,
            Channel channel,     //通信通道，用于手动确认信息
            Message message     //消息对象，包含消息的属性和体
    ) throws IOException {
        long messageTag = message.getMessageProperties().getDeliveryTag();
        RLock lock = redissonClient.getLock(
                ORDER_LOCK_PREFIX + voucherOrder.getUserId() + ":" + voucherOrder.getVoucherId());
        boolean locked = false;
        try {
            // 创建订单和补偿使用同一把锁，避免补偿与数据库事务提交并发发生。
            locked = lock.tryLock(5, 30, TimeUnit.SECONDS);
            if (!locked) {
                channel.basicNack(messageTag, false, true);
                return;
            }
            voucherOrderService.voucherOrder(voucherOrder);
            // voucherOrder() 返回时事务已经提交，再把 Redis 预留标记为已创建。
            voucherOrderService.markOrderCreated(voucherOrder);
            channel.basicAck(messageTag, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.basicNack(messageTag, false, true);
        } catch (Exception e) {
            log.error("订单消息消费失败，进入死信队列，orderId={}", voucherOrder.getId(), e);
            // 不重新入队，由订单队列的死信配置转发到死信交换机
            channel.basicNack(messageTag, false, false);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @RabbitListener(queues = RabbitMqConfig.ORDER_DEAD_QUEUE)
    public void handleDeadVoucherOrder(
            VoucherOrder voucherOrder,
            Channel channel,
            Message message
    ) throws IOException {
        long messageTag = message.getMessageProperties().getDeliveryTag();
        try {
            voucherOrderService.compensateRedisStock(voucherOrder);
            channel.basicAck(messageTag, false);
        } catch (Exception e) {
            log.error("死信订单补偿失败，orderId={}", voucherOrder.getId(), e);
            channel.basicNack(messageTag, false, true);
        }
    }

}
