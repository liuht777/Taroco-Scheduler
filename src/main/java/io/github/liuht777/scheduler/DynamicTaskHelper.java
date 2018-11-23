package io.github.liuht777.scheduler;

import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;


/**
 * 控制管理类
 * 该类的功能主要是对外提供的是一些操作任务和数据的方法，定时任务的增、删、查、改；
 * 定时任务状态查询
 *
 * @author liuht
 */
@Slf4j
public class DynamicTaskHelper {
    private static ZkClient zkClient;

    public static ZkClient getZkClient() {
        if (null == DynamicTaskHelper.zkClient) {
            synchronized (DynamicTaskHelper.class) {
                DynamicTaskHelper.zkClient = ZkClient.getApplicationcontext().getBean(ZkClient.class);
            }
        }
        return DynamicTaskHelper.zkClient;
    }

    public static void addScheduleTask(Task task) {
        try {
            log.info("添加任务：" + task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
            DynamicTaskHelper.getZkClient().getiScheduleTask().addTask(task);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void delScheduleTask(Task task) {
        try {
            log.info("删除任务：" + task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
            DynamicTaskHelper.getZkClient().getiScheduleTask().delTask(task);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void updateScheduleTask(Task task) {
        try {
            log.info("更新任务：" + task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
            DynamicTaskHelper.getZkClient().getiScheduleTask().updateTask(task);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static List<Task> queryScheduleTask() {
        List<Task> taskDefines = new ArrayList<Task>();
        try {
            List<Task> tasks = DynamicTaskHelper.getZkClient().getiScheduleTask().selectTask();
            taskDefines.addAll(tasks);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return taskDefines;
    }

    public static boolean isExistsTask(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return DynamicTaskHelper.getZkClient().getiScheduleTask().isExistsTask(task);
    }

    public static Task queryScheduleTask(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return DynamicTaskHelper.getZkClient().getiScheduleTask().selectTask(task);
    }


    public static boolean isOwner(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return DynamicTaskHelper.getZkClient().getiSchedulerServer().isOwner(task.stringKey(),
                ScheduleServer.getInstance().getUuid());
    }

    public static boolean isRunning(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return isExistsTask(task) && DynamicTaskHelper.getZkClient().getiScheduleTask().isRunning(task.stringKey());
    }
}
