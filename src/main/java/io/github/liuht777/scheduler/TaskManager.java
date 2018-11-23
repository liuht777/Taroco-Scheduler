package io.github.liuht777.scheduler;

import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.ScheduledMethodRunnable;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.util.ScheduleUtil;
import io.github.liuht777.scheduler.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * 动态任务管理
 *
 * @author liuht
 */
@Slf4j
public class TaskManager {

    /**
     * 缓存 scheduleKey 与 ScheduledFuture, 删除任务时可以优雅的关闭任务
     */
    private static final Map<String, ScheduledFuture<?>> SCHEDULE_FUTURES = new ConcurrentHashMap<>();

    /**
     * 缓存 scheduleKey 与 TaskDefine任务具体信息
     */
    private static final Map<String, Task> TASKS = new ConcurrentHashMap<>();

    /**
     * 添加并且启动定时任务
     * 需要添加同步锁
     *
     * @param task  定时任务
     */
    public synchronized static void scheduleTask(Task task) {
        log.debug("开始启动任务: " + task.stringKey());
        boolean newTask = true;
        if (SCHEDULE_FUTURES.containsKey(task.stringKey())) {
            if (task.equals(TASKS.get(task.stringKey()))) {
                log.debug("任务已经存在: " + task.stringKey() + ", 不需要重新构建");
                newTask = false;
            }
        }
        if (newTask) {
            scheduleTask(
                    task.getTargetBean(),
                    task.getTargetMethod(),
                    task.getCronExpression(),
                    task.getStartTime(),
                    task.getPeriod(),
                    task.getParams(),
                    task.getExtKeySuffix());
            TASKS.put(task.stringKey(), task);
            log.debug("成功添加任务: " + task.stringKey());
        }
    }

    /**
     * 清理本地任务
     *
     * @param existsTaskName 与本地缓存的任务列表 两者进行比对
     *                       如果远程没有了,本地还有,就要清除本地数据
     *                       并且停止本地任务cancel(true)
     */
    public static void clearLocalTask(List<String> existsTaskName) {
        for (String name : SCHEDULE_FUTURES.keySet()) {
            if (!existsTaskName.contains(name)) {
                SCHEDULE_FUTURES.get(name).cancel(true);
                SCHEDULE_FUTURES.remove(name);
                TASKS.remove(name);
                log.info("清理任务: " + name);
            }
        }
    }

    /**
     * 启动动态定时任务
     * 支持：
     * 1 cron时间表达式，立即执行
     * 2 startTime + period,指定时间，定时进行
     * 3 period，定时进行，立即开始
     * 4 startTime，指定时间执行
     *
     * @param targetBean     目标bean名称
     * @param targetMethod   方法
     * @param cronExpression cron表达式
     * @param startTime      指定执行时间
     * @param period         定时进行，立即开始
     * @param params         给方法传递的参数
     * @param extKeySuffix   任务后缀名
     */
    private static void scheduleTask(String targetBean, String targetMethod, String cronExpression, Date startTime, long
            period, String params, String extKeySuffix) {
        String scheduleKey = ScheduleUtil.buildScheduleKey(targetBean, targetMethod, extKeySuffix);
        try {
            if (!SCHEDULE_FUTURES.containsKey(scheduleKey)) {
                ScheduledFuture<?> scheduledFuture = null;
                ScheduledMethodRunnable scheduledMethodRunnable = buildScheduledRunnable(targetBean, targetMethod, params, extKeySuffix);
                if (scheduledMethodRunnable != null) {
                    if (StringUtils.isNotEmpty(cronExpression)) {
                        Trigger trigger = new CronTrigger(cronExpression);
                        scheduledFuture = TaskHelper.getZkClient().getTaskGenerator().schedule(scheduledMethodRunnable,
                                trigger);
                    } else if (startTime != null) {
                        if (period > 0) {
                            scheduledFuture = TaskHelper.getZkClient().getTaskGenerator()
                                    .scheduleAtFixedRate(scheduledMethodRunnable, startTime, period);
                        } else {
                            scheduledFuture = TaskHelper.getZkClient().getTaskGenerator()
                                    .schedule(scheduledMethodRunnable, startTime);
                        }
                    } else if (period > 0) {
                        scheduledFuture = TaskHelper.getZkClient().getTaskGenerator()
                                .scheduleAtFixedRate(scheduledMethodRunnable, period);
                    }
                    if (null != scheduledFuture) {
                        SCHEDULE_FUTURES.put(scheduleKey, scheduledFuture);
                        log.debug("成功启动动态任务, bean=" + targetBean + ", method=" + targetMethod +", params=" + params);
                    }
                } else {
                    TaskHelper.getZkClient().getiScheduleTask()
                            .saveRunningInfo(scheduleKey, ScheduleServer.getInstance().getUuid(), "bean not exists");
                    log.debug("启动动态任务失败: Bean name is not exists.");
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 封装ScheduledMethodRunnable对象
     */
    private static ScheduledMethodRunnable buildScheduledRunnable(String targetBean, String targetMethod, String params, String extKeySuffix) {
        Object bean;
        ScheduledMethodRunnable scheduledMethodRunnable = null;
        try {
            bean = ZkClient.getApplicationcontext().getBean(targetBean);
            scheduledMethodRunnable = buildScheduledRunnable(bean, targetMethod, params, extKeySuffix);
        } catch (Exception e) {
            String name = ScheduleUtil.buildScheduleKey(targetBean, targetMethod, extKeySuffix);
            try {
                TaskHelper.getZkClient().getiScheduleTask().saveRunningInfo(name,
                        ScheduleServer.getInstance().getUuid(), "method is null");
            } catch (Exception e1) {
                log.debug(e.getLocalizedMessage(), e);
            }
            log.debug(e.getLocalizedMessage(), e);
        }
        return scheduledMethodRunnable;
    }

    /**
     * 封装ScheduledMethodRunnable对象
     */
    private static ScheduledMethodRunnable buildScheduledRunnable(Object bean, String targetMethod, String params, String extKeySuffix) {

        Assert.notNull(bean, "target object must not be null");
        Assert.hasLength(targetMethod, "Method name must not be empty");

        Method method;
        ScheduledMethodRunnable scheduledMethodRunnable;
        Class<?> clazz;
        if (AopUtils.isAopProxy(bean)) {
            clazz = AopProxyUtils.ultimateTargetClass(bean);
        } else {
            clazz = bean.getClass();
        }
        if (params != null) {
            method = ReflectionUtils.findMethod(clazz, targetMethod, String.class);
        } else {
            method = ReflectionUtils.findMethod(clazz, targetMethod);
        }
        Assert.notNull(method, "can not find method named " + targetMethod);
        scheduledMethodRunnable = new ScheduledMethodRunnable(bean, method, params, extKeySuffix);
        return scheduledMethodRunnable;
    }
}
