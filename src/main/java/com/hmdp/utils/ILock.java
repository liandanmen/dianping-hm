package com.hmdp.utils;

public interface ILock {
    /*
    * 尝试获取分布式锁
    * */
    boolean tryLock(long timeoutSec);

    /*
    * 释放分布式锁
    * */
    void unlock();
}
