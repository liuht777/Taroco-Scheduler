package cn.uncode.schedule;

import cn.uncode.schedule.core.TaskDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


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
    
    static Properties properties = new Properties();
    
    public static void setProperties(Properties prop){
    	properties.putAll(prop);
    }
    
    public static SchedulerTaskManager getSchedulerTaskManager(){
    	if(null == ConsoleManager.schedulerTaskManager){
			synchronized(ConsoleManager.class) {
				ConsoleManager.schedulerTaskManager = SchedulerTaskManager.getApplicationcontext().getBean(SchedulerTaskManager.class);
			}
    	}
        return ConsoleManager.schedulerTaskManager;
    }

    public static void addScheduleTask(TaskDefine taskDefine) {
        try {
        	log.info("添加任务："+taskDefine.stringKey());
            Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().addTask(taskDefine);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    public static void delScheduleTask(TaskDefine taskDefine) {
        try {
			log.info("删除任务："+taskDefine.stringKey());
            Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().delTask(taskDefine);
			// 删除任务后出发本地任务检查
            ConsoleManager.getSchedulerTaskManager().getSchedulerServer().triggerTaskModified(
                    ConsoleManager.getSchedulerTaskManager().getTaskTrigger(), taskDefine.stringKey());
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    public static void updateScheduleTask(TaskDefine taskDefine) {
        try {
            log.info("更新任务："+taskDefine.stringKey());
            Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
            Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
			ConsoleManager.getSchedulerTaskManager().getScheduleTask().updateTask(taskDefine);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    public static List<TaskDefine> queryScheduleTask() {
    	List<TaskDefine> taskDefines = new ArrayList<TaskDefine>();
        try {
			List<TaskDefine> tasks = ConsoleManager.getSchedulerTaskManager().getScheduleTask().selectTask();
			taskDefines.addAll(tasks);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
        return taskDefines;
    }
    
    public static boolean isExistsTask(TaskDefine taskDefine){
        Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
        return ConsoleManager.getSchedulerTaskManager().getScheduleTask().isExistsTask(taskDefine);
    }
    
    public static TaskDefine queryScheduleTask(TaskDefine taskDefine) {
        Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
		return ConsoleManager.getSchedulerTaskManager().getScheduleTask().selectTask(taskDefine);
    }
    
    
    public static boolean isOwner(TaskDefine taskDefine){
        Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
		return ConsoleManager.getSchedulerTaskManager().getSchedulerServer().isOwner(taskDefine.stringKey(),
				ConsoleManager.getSchedulerTaskManager().getScheduleServerUUid());
    }

    public static boolean isRunning(TaskDefine taskDefine) {
        Assert.notNull(taskDefine.getTargetBean(), "targetBean can not be null.");
        Assert.notNull(taskDefine.getTargetMethod(), "targetMethod can not be null.");
        return isExistsTask(taskDefine) && ConsoleManager.getSchedulerTaskManager().getScheduleTask().isRunning(taskDefine.stringKey());
    }
}
