package com.fym;

/**
 * Created by fengyiming on 2018/8/7.
 * 获取锁后执行的方法
 */
@FunctionalInterface
public interface LockFunction {

    /**
     * 获取锁后执行的方法
     *
     * @return
     * @throws Exception
     */
    void lock() throws Exception;
}
