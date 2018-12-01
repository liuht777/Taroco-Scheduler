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

    private ApplicationContext applicationcontext;

    private IScheduleTask scheduleTask;

    private ISchedulerServer schedulerServer;

    public ThreadPoolTaskGenerator(int poolsize,
                                   IScheduleTask scheduleTask,
                                   ISchedulerServer schedulerServer) {
        this.setPoolSize(poolsize);
        this.scheduleTask = scheduleTask;
        this.schedulerServer = schedulerServer;
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
                            log.debug("任务[" + name + "] 成功触发!");
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

    /**
     * 根据Runnable 返回task相关信息
     *
     * @param runnable Runnable
     * @return Task
     */
    private Task resolveTaskName(final Runnable runnable) {
        Method targetMethod;
        Task task = new Task();
        if (runnable instanceof ScheduledMethodRunnable) {
            ScheduledMethodRunnable scheduledMethodRunnable = (ScheduledMethodRunnable) runnable;
            targetMethod = scheduledMethodRunnable.getMethod();
            task.setType(DefaultConstants.TYPE_TAROCO_TASK);
            if (StringUtils.isNotBlank(scheduledMethodRunnable.getExtKeySuffix())) {
                task.setExtKeySuffix(scheduledMethodRunnable.getExtKeySuffix());
            }
            if (StringUtils.isNotEmpty(scheduledMethodRunnable.getParams())) {
                task.setParams(scheduledMethodRunnable.getParams());
            }
        } else {
            org.springframework.scheduling.support.ScheduledMethodRunnable springScheduledMethodRunnable = (org.springframework.scheduling.support.ScheduledMethodRunnable) runnable;
            targetMethod = springScheduledMethodRunnable.getMethod();
            task.setType(DefaultConstants.TYPE_SPRING_TASK);
        }
        String[] beanNames = applicationcontext.getBeanNamesForType(targetMethod.getDeclaringClass());
        if (StringUtils.isNotEmpty(beanNames[0])) {
            task.setTargetBean(beanNames[0]);
            task.setTargetMethod(targetMethod.getName());
        }
        return task;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationcontext) throws BeansException {
        this.applicationcontext = applicationcontext;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(new Date(System.currentTimeMillis()));
                    task.setPeriod(period);
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.scheduleAtFixedRate(taskWrapper(runnable), period);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }

        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable runnable, Trigger trigger) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(new Date(System.currentTimeMillis()));
                    String cronEx = trigger.toString();
                    int index = cronEx.indexOf(":");
                    if (index >= 0) {
                        cronEx = cronEx.substring(index + 1);
                        task.setCronExpression(cronEx.trim());
                    }
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.schedule(taskWrapper(runnable), trigger);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable runnable, Date startTime) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(startTime);
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.schedule(taskWrapper(runnable), startTime);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, Date startTime, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(startTime);
                    task.setPeriod(period);
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.scheduleAtFixedRate(taskWrapper(runnable), startTime, period);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, Date startTime, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(startTime);
                    task.setPeriod(delay);
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(runnable), startTime, delay);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            Task task = resolveTaskName(runnable);
            if (task.getType().equals(DefaultConstants.TYPE_SPRING_TASK)) {
                // Spring 本地任务需要先添加到集群管理, 由集群统一分配后再执行
                if (!scheduleTask.isExistsTask(task)) {
                    task.setStartTime(new Date(System.currentTimeMillis()));
                    task.setPeriod(delay);
                    scheduleTask.addTask(task);
                }
            } else {
                // 动态任务直接执行
                scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(runnable), delay);
                log.info("添加 Taroco 动态任务[" + task.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    public ISchedulerServer getSchedulerServer() {
        return schedulerServer;
    }

    public IScheduleTask getScheduleTask() {
        return scheduleTask;
    }
}
