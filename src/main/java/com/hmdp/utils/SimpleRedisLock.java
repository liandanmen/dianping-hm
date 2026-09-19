package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class SimpleRedisLock implements ILock {

    private String lockKey;
    private StringRedisTemplate redisTemplate;
    private static final String ID_PREFIX = "lock:";
    private static final String LOCK_PREFIX = UUID.randomUUID().toString(true) + "_" ;

    //加载luo脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT=new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    public SimpleRedisLock(String lockKey, StringRedisTemplate redisTemplate) {
        this.lockKey = lockKey;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程的标识
        String threadId =LOCK_PREFIX+ Thread.currentThread().getId();
        // 尝试获取锁
        Boolean flag = redisTemplate.opsForValue()
                .setIfAbsent(ID_PREFIX+lockKey, threadId, timeoutSec, TimeUnit.SECONDS);
        if (flag != null && flag) {
            return true;
        }
        return false;
    }

    @Override
    public void unlock() {
        String threadId =LOCK_PREFIX+ Thread.currentThread().getId();
        redisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(ID_PREFIX+lockKey),
                threadId);
    }
   /* public void unlock() {
        //获取线程标识
        String threadId = LOCK_PREFIX+Thread.currentThread().getId();
        //判断是否一致
        if (threadId.equals(redisTemplate.opsForValue().get(lockKey))) {
            //一致则删除
            redisTemplate.delete(lockKey);
        }


    }*/
}
