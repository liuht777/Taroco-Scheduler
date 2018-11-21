package io.github.liuht777.scheduler;

import io.github.liuht777.scheduler.config.TarocoSchedulerProperties;
import io.github.liuht777.scheduler.core.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class ConsoleManager {

    private static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

    private static SchedulerTaskManager schedulerTaskManager;

    static TarocoSchedulerProperties schedulerProperties;

    public static void setProperties(TarocoSchedulerProperties schedulerProperties){
    	schedulerProperties = schedulerProperties;
    }

    public static SchedulerTaskManager getSchedulerTaskManager(){
    	if(null == ConsoleManager.schedulerTaskManager){
			synchronized(ConsoleManager.class) {
				ConsoleManager.schedulerTaskManager = SchedulerTaskManager.getApplicationcontext().getBean(SchedulerTaskManager.class);
			}
    	}
        return ConsoleManager.schedulerTaskManager;
    }

    public static void addScheduleTask(Task task) {
        try {
        	log.info("添加任务："+ task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().addTask(task);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }

    public static void delScheduleTask(Task task) {
        try {
			log.info("删除任务："+ task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().delTask(task);
			// 删除任务后出发本地任务检查
            ConsoleManager.getSchedulerTaskManager().getSchedulerServer().triggerTaskModified(
                    ConsoleManager.getSchedulerTaskManager().getTaskTrigger(), task.stringKey());
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }

    public static void updateScheduleTask(Task task) {
        try {
            log.info("更新任务："+ task.stringKey());
            Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().updateTask(task);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }

    public static List<Task> queryScheduleTask() {
    	List<Task> taskDefines = new ArrayList<Task>();
        try {
			List<Task> tasks = ConsoleManager.getSchedulerTaskManager().getScheduleTask().selectTask();
			taskDefines.addAll(tasks);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
        return taskDefines;
    }

    public static boolean isExistsTask(Task task){
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return ConsoleManager.getSchedulerTaskManager().getScheduleTask().isExistsTask(task);
    }

    public static Task queryScheduleTask(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
		return ConsoleManager.getSchedulerTaskManager().getScheduleTask().selectTask(task);
    }


    public static boolean isOwner(Task task){
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
		return ConsoleManager.getSchedulerTaskManager().getSchedulerServer().isOwner(task.stringKey(),
				ConsoleManager.getSchedulerTaskManager().getCurrentScheduleServerUUid());
    }

    public static boolean isRunning(Task task) {
        Assert.notNull(task.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(task.getTargetMethod(), "targetMethod can not be null.");
        return isExistsTask(task) && ConsoleManager.getSchedulerTaskManager().getScheduleTask().isRunning(task.stringKey());
    }
}
