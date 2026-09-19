package com.hmdp.utils;

import com.hmdp.config.RabbitMqConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;


// 消费者类，处理优惠券订单
@Component
public class VoucherOrderConsumer {
    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMqConfig.ORDER_QUEUE)
    public void handleVouherOrder(
            VoucherOrder voucherOrder,
            Channel channel,     //通信通道，用于手动确认信息
            Message message     //消息对象，包含消息的属性和体
    ) throws IOException {
        long messageTag=message.getMessageProperties().getDeliveryTag();
        try {
            voucherOrderService.voucherOrder(voucherOrder);
            channel.basicAck(messageTag, false);
        } catch (Exception e) {
            channel.basicNack(messageTag, false,true);
        }

    }

}
