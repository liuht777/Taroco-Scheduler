package io.github.liuht777.scheduler.rule.impl;

import io.github.liuht777.scheduler.rule.AssignServerRole;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询分配server 原子类实现线程安全的轮询负载均衡算法
 *
 * @author liuht
 * 2018/12/3 16:09
 */
public class PollingAssignServerRole implements AssignServerRole {

    /**
     * 当前轮询索引
     */
    private AtomicInteger index = new AtomicInteger(0);

    @Override
    public String doSelect(final List<String> serverList) {
        if (index.get() >= serverList.size()) {
            index.set(0);
        }
        return serverList.get(index.getAndIncrement());
    }
}
