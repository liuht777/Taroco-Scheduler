package io.github.liuht777.scheduler;

import io.github.liuht777.scheduler.config.TarocoSchedulerProperties;
import io.github.liuht777.scheduler.constant.DefaultConstants;
import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.ScheduledMethodRunnable;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.zookeeper.ScheduleTask;
import io.github.liuht777.scheduler.zookeeper.SchedulerServer;
import io.github.liuht777.scheduler.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_SERVER;
import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_TASK;

/**
 * 调度器核心管理
 *
 * @author liuht
 */
@Slf4j
public class SchedulerTaskManager extends ThreadPoolTaskScheduler implements ApplicationContextAware {

    private static final long serialVersionUID = 8048640374020873814L;
    private static ApplicationContext applicationcontext;
    /**
     * server 根节点
     */
    private String pathServer;
    /**
     * task 根节点
     */
    private String pathTask;
    /**
     * 配置properties
     */
    private TarocoSchedulerProperties schedulerProperties;
    /**
     * zookeeper 客户端
     */
    private ZkClient zkClient;
    /**
     * 自定义任务task
     */
    private IScheduleTask scheduleTask;
    /**
     * 自定义任务服务
     */
    private ISchedulerServer schedulerServer;
    /**
     * 当前调度服务的信息
     */
    private ScheduleServer currenScheduleServer;

    public SchedulerTaskManager(String ownSign, TarocoSchedulerProperties schedulerProperties) {
        // 初始化调度服务器
        this.currenScheduleServer = ScheduleServer.createScheduleServer(ownSign);
        this.schedulerProperties = schedulerProperties;
    }

    public static ApplicationContext getApplicationcontext() {
        return SchedulerTaskManager.applicationcontext;
    }

    /**
     * 调度器核心管理 配置参数初始化
     */
    public void init() {
        final String rootPath = this.schedulerProperties.getZk().getRootPath();
        this.pathTask = rootPath + "/" + NODE_TASK;
        this.pathServer = rootPath + "/" + NODE_SERVER;
        this.setPoolSize(this.schedulerProperties.getPoolSize());
        // 初始化 zookeeper 连接
        this.zkClient = new ZkClient(this.schedulerProperties);
        this.zkClient.getClient().getConnectionStateListenable().addListener((curatorFramework, state) -> {
            switch (state) {
                case RECONNECTED:
                    // 挂起或者丢失连接后重新连接
                    log.info("reconnected with zookeeper");
                    initialData();
                    break;
                default:
                    break;
            }
        });
        initialData();
    }

    /**
     * 在Zk状态正常后回调数据初始化
     */
    public void initialData() {
        this.scheduleTask = new ScheduleTask(this.zkClient.getClient(), this.pathTask);
        this.schedulerServer = new SchedulerServer(this.zkClient, this.pathServer, this.pathTask);
        // 监听配置
        this.initPathAndWatchServer(this.pathServer);
        this.initPathAndWatchTask(this.pathTask);
        // 注册当前server
        this.schedulerServer.registerScheduleServer(this.currenScheduleServer);
    }

    /**
     * 初始化并且监听task节点
     * 触发检查本地任务
     */
    private void initPathAndWatchTask(String pathTask) {
        try {
            if (this.zkClient.getClient().checkExists().forPath(pathTask) == null) {
                this.zkClient.getClient().create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT).forPath(pathTask);
            }
            // 监听子节点变化情况
            final PathChildrenCache watcher = new PathChildrenCache(this.zkClient.getClient(), pathTask, true);
            watcher.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            watcher.getListenable().addListener(
                    (client, event) -> {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                log.info("监听到task节点变化: 新增task=: {}", event.getData().getPath());
                                assignScheduleTask();
                                checkLocalTask();
                                break;
                            case CHILD_REMOVED:
                                log.info("监听到task节点变化: 删除task=: {}", event.getData().getPath());
                                assignScheduleTask();
                                checkLocalTask();
                                break;
                            default:
                                break;
                        }
                    }
            );
        } catch (Exception e) {
            log.error("initPathAndWatchTask failed", e);
        }
    }

    /**
     * 初始化并且监听server节点
     * 触发重新分配任务
     */
    private void initPathAndWatchServer(String pathServer) {
        try {
            if (this.zkClient.getClient().checkExists().forPath(pathServer) == null) {
                this.zkClient.getClient().create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT).forPath(pathServer);
            }
            // 监听子节点变化情况
            final PathChildrenCache watcher = new PathChildrenCache(this.zkClient.getClient(), pathServer, true);
            watcher.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            watcher.getListenable().addListener(
                    (client, event) -> {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                log.info("监听到server节点变化: 新增server=: {}", event.getData().getPath());
                                assignScheduleTask();
                                checkLocalTask();
                                break;
                            case CHILD_REMOVED:
                                log.info("监听到server节点变化: 删除server=: {}", event.getData().getPath());
                                assignScheduleTask();
                                checkLocalTask();
                                break;
                            default:
                                break;
                        }
                    }
            );
        } catch (Exception e) {
            log.error("initPathAndWatchTask failed", e);
        }
    }

    /**
     * 根据当前调度服务器的信息，重新计算分配所有的调度任务 任务的分配是需要加锁，避免数据分配错误。
     */
    public void assignScheduleTask() {
        List<String> serverList = schedulerServer.loadScheduleServerNames();
        //黑名单
        for (String ip : schedulerProperties.getIpBlackList()) {
            int index = serverList.indexOf(ip);
            if (index > -1) {
                serverList.remove(index);
            }
        }
        // 执行任务分配
        schedulerServer.assignTask(this.currenScheduleServer.getUuid(), serverList);
    }

    /**
     * 检查/执行 本地任务
     */
    public void checkLocalTask() {
        schedulerServer.checkLocalTask(this.currenScheduleServer.getUuid());
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
                    isOwner = schedulerServer.isOwner(name, currenScheduleServer.getUuid());
                    isRunning = scheduleTask.isRunning(name);
                    if (isOwner && isRunning) {
                        String errorMsg = null;
                        try {
                            runnable.run();
                            log.info("任务[" + name + "] 成功触发!");
                        } catch (Exception e) {
                            errorMsg = e.getLocalizedMessage();
                        }
                        scheduleTask.saveRunningInfo(name, currenScheduleServer.getUuid(), errorMsg);
                    } else {
                        if (!isOwner) {
                            log.debug("任务[" + name + "] 触发失败, 不属于当前server[" + currenScheduleServer.getUuid() + "]");
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

    public IScheduleTask getScheduleTask() {
        return scheduleTask;
    }

    public ISchedulerServer getSchedulerServer() {
        return schedulerServer;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationcontext)
            throws BeansException {
        SchedulerTaskManager.applicationcontext = applicationcontext;
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
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
                log.info(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
            }
        } catch (Exception e) {
            log.error("update task error", e);
        }
        return scheduledFuture;
    }

    public String getCurrentScheduleServerUUid() {
        if (null != currenScheduleServer) {
            return currenScheduleServer.getUuid();
        }
        return null;
    }

    @Override
    public void destroy() {
        if (this.zkClient.getClient() != null) {
            try {
                this.zkClient.getClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
