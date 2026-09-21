package com.hmdp.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMqConfig {

    public static final String ORDER_QUEUE = "hmdp.order.queue";
    public static final String ORDER_EXCHANGE = "hmdp.order.exchange";
    public static final String ORDER_ROUTING_KEY = "voucher.order";
    public static final String ORDER_DEAD_EXCHANGE = "hmdp.order.dead.exchange";
    public static final String ORDER_DEAD_QUEUE = "hmdp.order.dead.queue";
    public static final String ORDER_DEAD_ROUTING_KEY = "voucher.order.dead";

    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange(ORDER_EXCHANGE);
    }

    @Bean
    public Queue orderQueue() {
        //订单绑定
        // 设置死信交换机和死信路由键
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-dead-letter-exchange", ORDER_DEAD_EXCHANGE);
        arguments.put("x-dead-letter-routing-key", ORDER_DEAD_ROUTING_KEY);
        // 设置队列的其他属性，如是否持久化、是否自动删除等，true表示持久化
        return new Queue(ORDER_QUEUE, true, false, false, arguments);
    }


    @Bean
    public Binding orderBinding(
            @Qualifier("orderQueue") Queue orderQueue,
            @Qualifier("orderExchange") DirectExchange orderExchange) {
        return BindingBuilder
                .bind(orderQueue)
                .to(orderExchange)
                .with(ORDER_ROUTING_KEY);
    }

    //声明死信交换机
    @Bean
    public DirectExchange orderDeadExchange() {
        return new DirectExchange(ORDER_DEAD_EXCHANGE);
    }

    //声明死信队列
    @Bean
    public Queue orderDeadQueue() {
        return new Queue(ORDER_DEAD_QUEUE, true);
    }

    //声明死信队列绑定，失败消息最终放这里
    @Bean
    public Binding orderDeadBinding(
            @Qualifier("orderDeadQueue") Queue orderDeadQueue,
            @Qualifier("orderDeadExchange") DirectExchange orderDeadExchange) {
        return BindingBuilder
                .bind(orderDeadQueue)
                .to(orderDeadExchange)
                .with(ORDER_DEAD_ROUTING_KEY);
    }

    //声明消息转换器
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
