package io.github.liuht777.scheduler;

import io.github.liuht777.scheduler.constant.DefaultConstants;
import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.ScheduledMethodRunnable;
import io.github.liuht777.scheduler.core.Task;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;


/**
 * 定时任务生成器
 * 继承 {@link ThreadPoolTaskScheduler}, Spring Task 定时任务的默认实现
 *
 * @author liuht
 */
@Slf4j
public class ThreadPoolTaskGenerator extends ThreadPoolTaskScheduler implements ApplicationContextAware {

    private static final long serialVersionUID = 8048640374020873814L;

    private static ApplicationContext applicationcontext;

    private IScheduleTask scheduleTask;

    private ISchedulerServer schedulerServer;

    public ThreadPoolTaskGenerator(int poolsize,
                                   IScheduleTask scheduleTask,
                                   ISchedulerServer schedulerServer) {
        this.setPoolSize(poolsize);
        this.scheduleTask = scheduleTask;
        this.schedulerServer = schedulerServer;
    }

    public static ApplicationContext getApplicationcontext() {
        return ThreadPoolTaskGenerator.applicationcontext;
    }

    /**
     * task runnable封装
     *
     * @param runnable
     * @return
     */
    private Runnable taskWrapper(final Runnable runnable) {
        return () -> {
            Task task = resolveTaskName(runnable);
            String name = task.stringKey();
            if (StringUtils.isNotEmpty(name)) {
                boolean isOwner;
                boolean isRunning;
                try {
                    isOwner = schedulerServer.isOwner(name, ScheduleServer.getInstance().getUuid());
                    isRunning = scheduleTask.isRunning(name);
                    if (isOwner && isRunning) {
                        String errorMsg = null;
                        try {
                            runnable.run();
                            log.info("任务[" + name + "] 成功触发!");
                        } catch (Exception e) {
                            errorMsg = e.getLocalizedMessage();
                        }
                        scheduleTask.saveRunningInfo(name, ScheduleServer.getInstance().getUuid(), errorMsg);
                    } else {
                        if (!isOwner) {
                            log.debug("任务[" + name + "] 触发失败, 不属于当前server[" + ScheduleServer.getInstance().getUuid() + "]");
                        }
                        if (!isRunning) {
                            log.debug("任务[" + name + "] 触发失败, 任务被暂停了");
                        }
                    }
                } catch (Exception e) {
                    log.error("Check task owner error.", e);
                }
            }
        };
    }

    private Task resolveTaskName(final Runnable task) {
        Method targetMethod;
        Task taskDefine = new Task();
        if (task instanceof ScheduledMethodRunnable) {
            ScheduledMethodRunnable uncodeScheduledMethodRunnable = (ScheduledMethodRunnable) task;
            targetMethod = uncodeScheduledMethodRunnable.getMethod();
            taskDefine.setType(DefaultConstants.TYPE_TAROCO_TASK);
            if (StringUtils.isNotBlank(uncodeScheduledMethodRunnable.getExtKeySuffix())) {
                taskDefine.setExtKeySuffix(uncodeScheduledMethodRunnable.getExtKeySuffix());
            }
        } else {
            org.springframework.scheduling.support.ScheduledMethodRunnable springScheduledMethodRunnable = (org.springframework.scheduling.support.ScheduledMethodRunnable) task;
            targetMethod = springScheduledMethodRunnable.getMethod();
            taskDefine.setType(DefaultConstants.TYPE_SPRING_TASK);
        }
        String[] beanNames = applicationcontext.getBeanNamesForType(targetMethod.getDeclaringClass());
        if (StringUtils.isNotEmpty(beanNames[0])) {
            taskDefine.setTargetBean(beanNames[0]);
            taskDefine.setTargetMethod(targetMethod.getName());
        }
        return taskDefine;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationcontext)
            throws BeansException {
        ThreadPoolTaskGenerator.applicationcontext = applicationcontext;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.scheduleAtFixedRate(task, period);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                taskDefine.setPeriod(period);
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.scheduleAtFixedRate(taskWrapper(task), period);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }

        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.schedule(task, trigger);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                String cronEx = trigger.toString();
                int index = cronEx.indexOf(":");
                if (index >= 0) {
                    cronEx = cronEx.substring(index + 1);
                    taskDefine.setCronExpression(cronEx.trim());
                }
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.schedule(taskWrapper(task), trigger);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.schedule(task, startTime);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                taskDefine.setStartTime(startTime);
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.schedule(taskWrapper(task), startTime);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.scheduleAtFixedRate(task, startTime, period);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                taskDefine.setStartTime(startTime);
                taskDefine.setPeriod(period);
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.scheduleAtFixedRate(taskWrapper(task), startTime, period);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.scheduleWithFixedDelay(task, startTime, delay);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                taskDefine.setStartTime(startTime);
                taskDefine.setPeriod(delay);
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(task), startTime, delay);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task taskDefine = resolveTaskName(task);
            if (taskDefine.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                super.scheduleWithFixedDelay(task, delay);
                log.info(":添加本地任务[" + taskDefine.stringKey() + "]");
            } else {
                taskDefine.setPeriod(delay);
                scheduleTask.addTask(taskDefine);
                scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(task), delay);
                log.info(ScheduleServer.getInstance().getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

}
