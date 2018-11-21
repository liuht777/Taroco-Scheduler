package io.github.liuht777.scheduler.core;

import java.util.List;


/**
 * 定义任务基本操作
 *
 * @author liuht
 * @date 2017/10/21 14:19
 */
public interface IScheduleTask {

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
     *                   targetBean 不能为空
     *                   targetMethod 不能为空
     */
    void addTask(Task task);

    /**
     * 更新指定任务
     *
     * @param task 任务详情
     *                   targetBean 不能为空
     *                   targetMethod 不能为空
     */
    void updateTask(Task task);

    /**
     * 删除指定任务
     *
     * @param task 任务详情
     *                   targetBean 不能为空
     *                   targetMethod 不能为空
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
     *                   targetBean 不能为空
     *                   targetMethod 不能为空
     * @return 是否存在
     */
    boolean isExistsTask(Task task);

    /**
     * 返回指定任务详情
     *
     * @param task 任务数据
     *                   targetBean 不能为空
     *                   targetMethod 不能为空
     * @return 任务详情
     */
    Task selectTask(Task task);

    /**
     * 保存任务执行信息
     *
     * @param taskName 任务名称
     * @param serverUuid 服务器唯一标识
     * @param msg 附加消息
     * @return 保存结果
     */
    boolean saveRunningInfo(String taskName, String serverUuid, String msg);

    /**
     * 保存任务执行信息
     *
     * @param taskName 任务名称
     * @param serverUuid 服务器唯一标识
     * @return 保存结果
     */
    boolean saveRunningInfo(String taskName, String serverUuid);
}
