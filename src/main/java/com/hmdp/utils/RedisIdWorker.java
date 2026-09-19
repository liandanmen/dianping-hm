package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long START_TIME = 1700080800L;
    private static final long COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    //生成全局唯一id
    public long nextID(String keyPrefix){
        //生成时间戳
        long timestamp = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - START_TIME;
        //获取序列号，使用redis自增长
        DateTimeFormatter date = DateTimeFormatter.ofPattern("yyyy:MM:dd");
        long increment = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix+":"+date.format(LocalDateTime.now()));

        //拼接并且返回
        return timestamp << COUNT_BITS | increment;

    }




}
