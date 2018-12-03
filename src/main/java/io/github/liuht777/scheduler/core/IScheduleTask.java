package io.github.liuht777.scheduler.core;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;


/**
 * 定义任务task的接口定义
 *
 * @author liuht
 * @date 2017/10/21 14:19
 */
public interface IScheduleTask {

    /**
     * 设置 CuratorFramework
     *
     * @param client zk client对象
     */
    void setClient(CuratorFramework client);

    /**
     * 返回指定任务是否是执行状态
     *
     * @param taskName 任务唯一标识
     * @return 是否是执行状态
     */
    boolean isRunning(String taskName);

    /**
     * 添加指定任务
     *
     * @param task 任务详情
     */
    void addTask(Task task);

    /**
     * 更新指定任务
     *
     * @param task 任务详情
     */
    void updateTask(Task task);

    /**
     * 删除指定任务
     *
     * @param task 任务详情
     */
    void delTask(Task task);

    /**
     * 返回所有任务
     *
     * @return List<Task>
     */
    List<Task> selectTask();

    /**
     * 判断指定任务是否存在
     *
     * @param task 任务详情
     * @return 是否存在
     */
    boolean isExistsTask(Task task);

    /**
     * 返回指定任务详情
     *
     * @param task 任务数据
     * @return 任务详情
     */
    Task selectTask(Task task);

    /**
     * 保存任务执行信息
     *
     * @param taskName   任务名称
     * @param serverUuid 服务器唯一标识
     * @param errorMsg   错误消息, 标识任务执行失败
     * @return 保存结果
     */
    boolean saveRunningInfo(String taskName, String serverUuid, String errorMsg);

    /**
     * 保存任务执行信息
     *
     * @param taskName   任务名称
     * @param serverUuid 服务器唯一标识
     * @return 保存结果
     */
    boolean saveRunningInfo(String taskName, String serverUuid);
}
