package io.github.liuht777.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.ScheduledMethodRunnable;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.event.AssignScheduleTaskEvent;
import io.github.liuht777.scheduler.util.JsonUtil;
import io.github.liuht777.scheduler.util.ScheduleUtil;
import io.github.liuht777.scheduler.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 动态任务管理
 *
 * @author liuht
 */
@Slf4j
public class TaskManager implements ApplicationContextAware {

    /**
     * 缓存 scheduleKey 与 ScheduledFuture, 删除任务时可以优雅的关闭任务
     */
    private final Map<String, ScheduledFuture<?>> SCHEDULE_FUTURES = new ConcurrentHashMap<>();

    /**
     * 缓存 scheduleKey 与 TaskDefine任务具体信息
     */
    private final Map<String, Task> TASKS = new ConcurrentHashMap<>();

    private AtomicInteger pos = new AtomicInteger(0);

    private ApplicationContext applicationContext;

    private ZkClient zkClient;
    /**
     * 定时刷新/检查 线程池
     */
    private ScheduledExecutorService refreshTaskExecutor;

    public TaskManager(ZkClient zkClient) {
        this.zkClient = zkClient;
        this.refreshTaskExecutor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("ServerTaskCheckInterval").build());
        this.checkLocalTask();
    }

    /**
     * 根据当前调度服务器的信息，重新计算分配所有的调度任务 任务的分配是需要加锁，避免数据分配错误。
     */
    @EventListener
    public void assignScheduleTask(AssignScheduleTaskEvent event) {
        List<String> serverList = zkClient.getTaskGenerator().getSchedulerServer().loadScheduleServerNames();
        //黑名单
        for (String ip : zkClient.getSchedulerProperties().getIpBlackList()) {
            int index = serverList.indexOf(ip);
            if (index > -1) {
                serverList.remove(index);
            }
        }
        // 执行任务分配
        assignTask(ScheduleServer.getInstance().getUuid(), serverList);
    }

    /**
     * 分配任务
     *
     * @param currentUuid    当前服务器uuid
     * @param taskServerList 所有服务器uuid(过滤后的)
     */
    public void assignTask(String currentUuid, List<String> taskServerList) {
        if (CollectionUtils.isEmpty(taskServerList)) {
            log.info("当前Server List 为空, 暂不能分配任务...");
            return;
        }
        log.info("当前server:[" + currentUuid + "]: 开始重新分配任务......");
        if (!this.zkClient.getTaskGenerator().getSchedulerServer().isLeader(currentUuid, taskServerList)) {
            log.info("当前server:[" + currentUuid + "]: 不是负责任务分配的Leader,直接返回");
            return;
        }
        if (CollectionUtils.isEmpty(taskServerList)) {
            //在服务器动态调整的时候，可能出现服务器列表为空的情况
            log.info("服务器列表为空: 停止分配任务, 等待服务器上线...");
            return;
        }
        try {
            String zkPath = zkClient.getTaskPath();
            List<String> taskNames = zkClient.getClient().getChildren().forPath(zkPath);
            if (CollectionUtils.isEmpty(taskNames)) {
                log.info("当前server:[" + currentUuid + "]: 分配结束,没有集群任务");
                return;
            }
            for (String taskName : taskNames) {
                String taskPath = zkPath + "/" + taskName;
                List<String> taskServerIds = zkClient.getClient().getChildren().forPath(taskPath);
                if (CollectionUtils.isEmpty(taskServerIds)) {
                    // 没有找到目标server信息, 执行分配任务给server节点
                    assignServer2Task(taskServerList, taskPath);
                } else {
                    boolean hasAssignSuccess = false;
                    for (String serverId : taskServerIds) {
                        if (taskServerList.contains(serverId)) {
                            //防止重复分配任务，如果已经成功分配，第二个以后都删除
                            if (hasAssignSuccess) {
                                zkClient.getClient().delete().deletingChildrenIfNeeded()
                                        .forPath(taskPath + "/" + serverId);
                            } else {
                                hasAssignSuccess = true;
                            }
                        }
                    }
                    if (!hasAssignSuccess) {
                        assignServer2Task(taskServerList, taskPath);
                    }
                }
            }
        } catch (Exception e) {
            log.error("assignTask failed:", e);
        }
    }

    /**
     * 重新分配任务给server 采用轮询分配的方式
     * 分配任务操作是同步的
     *
     * @param taskServerList 待分配server列表
     * @param taskPath       任务path
     */
    private synchronized void assignServer2Task(List<String> taskServerList, String taskPath) {
        if (pos.intValue() > taskServerList.size() - 1) {
            pos.set(0);
        }
        // 轮询分配给server
        String serverId = taskServerList.get(pos.intValue());
        pos.incrementAndGet();
        try {
            if (zkClient.getClient().checkExists().forPath(taskPath) != null) {
                final String runningInfo = "0:" + System.currentTimeMillis();
                final String path = taskPath + "/" + serverId;
                final Stat stat = zkClient.getClient().checkExists().forPath(path);
                if (stat == null) {
                    zkClient.getClient()
                            .create()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(path, runningInfo.getBytes());
                }
                log.info("成功分配任务 [" + taskPath + "]" + " 给 server [" + serverId + "]");
            }
        } catch (Exception e) {
            log.error("assign task error", e);
        }
    }

    /**
     * 定时检查/执行 本地任务
     * 1. 清理过时的本地任务(zk上已经删除的)
     * 2. 添加执行分配给自己的任务
     */
    public void checkLocalTask() {
        refreshTaskExecutor.scheduleAtFixedRate(
                () -> checkLocalTask(ScheduleServer.getInstance().getUuid()),
                0, zkClient.getSchedulerProperties().getRefreshTaskInterval(), TimeUnit.SECONDS);
    }

    /**
     * 检查本地的定时任务，添加调度器；这是动态添加的任务的真正开始执行的地方
     * 如果有的话启动该定时任务；这是一种自定义的定时任务类型，任务的启动方式也是自定义的，主要方法在类 TaskManager 中；
     *
     * @param currentUuid 当前服务器唯一标识
     */
    public void checkLocalTask(String currentUuid) {
        try {
            String zkPath = zkClient.getTaskPath();
            List<String> taskNames = zkClient.getClient().getChildren().forPath(zkPath);
            if (CollectionUtils.isEmpty(taskNames)) {
                log.debug("当前server:[" + currentUuid + "]: 检查本地任务结束, 任务列表为空");
                return;
            }
            List<String> localTasks = new ArrayList<>();
            for (String taskName : taskNames) {
                if (zkClient.getTaskGenerator().getSchedulerServer().isOwner(taskName, currentUuid)) {
                    String taskPath = zkPath + "/" + taskName;
                    byte[] data = zkClient.getClient().getData().forPath(taskPath);
                    if (null != data) {
                        String json = new String(data);
                        Task td = JsonUtil.json2Object(json, Task.class);
                        Task task = new Task();
                        task.valueOf(td);
                        localTasks.add(taskName);
                        // 启动任务
                        scheduleTask(task);
                    }
                }
            }
            clearLocalTask(localTasks);
        } catch (Exception e) {
            log.error("checkLocalTask failed", e);
        }
    }

    /**
     * 添加并且启动定时任务
     * 需要添加同步锁
     *
     * @param task  定时任务
     */
    public synchronized void scheduleTask(Task task) {
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
    public void clearLocalTask(List<String> existsTaskName) {
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
    private void scheduleTask(String targetBean, String targetMethod, String cronExpression,
                                     Date startTime, Long period, String params, String extKeySuffix) {
        String scheduleKey = ScheduleUtil.buildScheduleKey(targetBean, targetMethod, extKeySuffix);
        try {
            if (!SCHEDULE_FUTURES.containsKey(scheduleKey)) {
                ScheduledFuture<?> scheduledFuture = null;
                ScheduledMethodRunnable scheduledMethodRunnable = buildScheduledRunnable(targetBean, targetMethod, params, extKeySuffix);
                if (scheduledMethodRunnable != null) {
                    if (StringUtils.isNotEmpty(cronExpression)) {
                        Trigger trigger = new CronTrigger(cronExpression);
                        scheduledFuture = zkClient.getTaskGenerator().schedule(scheduledMethodRunnable,
                                trigger);
                    } else if (startTime != null) {
                        if (period > 0) {
                            scheduledFuture = zkClient.getTaskGenerator().scheduleAtFixedRate(scheduledMethodRunnable, startTime, period);
                        } else {
                            scheduledFuture = zkClient.getTaskGenerator().schedule(scheduledMethodRunnable, startTime);
                        }
                    } else if (period > 0) {
                        scheduledFuture = zkClient.getTaskGenerator().scheduleAtFixedRate(scheduledMethodRunnable, period);
                    }
                    if (null != scheduledFuture) {
                        SCHEDULE_FUTURES.put(scheduleKey, scheduledFuture);
                        log.debug("成功启动动态任务, bean=" + targetBean + ", method=" + targetMethod +", params=" + params);
                    }
                } else {
                    zkClient.getTaskGenerator().getScheduleTask().saveRunningInfo(scheduleKey, ScheduleServer.getInstance().getUuid(), "bean not exists");
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
    private ScheduledMethodRunnable buildScheduledRunnable(String targetBean, String targetMethod, String params, String extKeySuffix) {
        Object bean;
        ScheduledMethodRunnable scheduledMethodRunnable = null;
        try {
            bean = applicationContext.getBean(targetBean);
            scheduledMethodRunnable = buildScheduledRunnable(bean, targetMethod, params, extKeySuffix);
        } catch (Exception e) {
            String name = ScheduleUtil.buildScheduleKey(targetBean, targetMethod, extKeySuffix);
            try {
                zkClient.getTaskGenerator().getScheduleTask().saveRunningInfo(name, ScheduleServer.getInstance().getUuid(), "method is null");
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
    private ScheduledMethodRunnable buildScheduledRunnable(Object bean, String targetMethod, String params, String extKeySuffix) {

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

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
