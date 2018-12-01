package io.github.liuht777.scheduler.core;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;

/**
 * server节点接口定义
 *
 * @author liuht
 * @date 2017/10/21 14:19
 */
public interface ISchedulerServer {

    /**
     * 判断指定任务 是否属于指定server节点
     *
     * @param taskName   任务唯一标识
     * @param serverUuid 服务器唯一标识
     * @return 是否属于指定服务器
     */
    boolean isOwner(String taskName, String serverUuid);

    /**
     * 设置 CuratorFramework
     * @param client zk client对象
     */
    void setClient(CuratorFramework client);

    /**
     * 注册服务器
     *
     * @param server 服务器信息
     */
    void registerScheduleServer(ScheduleServer server);

    /**
     * 判断该服务器是否是分布式调度中心
     *
     * @param serverUuid 服务器唯一标识
     * @param serverList 所有服务器
     * @return 指定服务器是否是分布式调度中心
     */
    boolean isLeader(String serverUuid, List<String> serverList);

    /**
     * 返回所有服务器名称
     *
     * @return 所有服务器名称
     */
    List<String> loadScheduleServerNames();
}
