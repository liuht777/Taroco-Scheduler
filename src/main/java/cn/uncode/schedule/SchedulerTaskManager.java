package cn.uncode.schedule;

import cn.uncode.schedule.core.*;
import cn.uncode.schedule.zk.ScheduleTaskForZookeeper;
import cn.uncode.schedule.zk.SchedulerServerForZookeeper;
import cn.uncode.schedule.zk.ZKManager;
import cn.uncode.schedule.zk.ZKTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调度器核心管理
 *
 * @author juny.ye
 */
public class SchedulerTaskManager extends ThreadPoolTaskScheduler implements ApplicationContextAware {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_POOL_SIZE = 20;

    private static final transient Logger LOGGER = LoggerFactory.getLogger(SchedulerTaskManager.class);

    private static final String NODE_TASK = "task";

    private static final String NODE_SERVER = "server";

    private String pathServer;

    private String pathTask;

    private Map<String, String> config;

    private ZKManager zkManager;

    private IScheduleTask scheduleTask;

    private ISchedulerServer schedulerServer;

    /**
     * 当前调度服务的信息
     */
    private ScheduleServer currenScheduleServer;

    /**
     * 是否启动调度管理，如果只是做系统管理，应该设置为false,对应key值为onlyAdmin
     */
    private boolean start = true;

    /**
     * 是否注册成功
     */
    private boolean isScheduleServerRegister = true;

    private static ApplicationContext applicationcontext;

    private Map<String, Boolean> isOwnerMap = new ConcurrentHashMap<String, Boolean>();

    private Lock initLock = new ReentrantLock();

    public SchedulerTaskManager() {
        this(null);
    }

    public SchedulerTaskManager(String ownSign) {
        // 初始化调度服务器
        this.currenScheduleServer = ScheduleServer.createScheduleServer(ownSign);
    }

    /**
     * 调度器核心管理 配置参数初始化
     *
     * @throws Exception 异常信息
     */
    public void init() throws Exception {
        if (this.config != null) {
            for (Map.Entry<String, String> e : this.config.entrySet()) {
                ConsoleManager.properties.put(e.getKey(), e.getValue());
            }
        }
        if (ConsoleManager.properties.containsKey("onlyAdmin")) {
            String val = String.valueOf(ConsoleManager.properties.get("onlyAdmin"));
            if (StringUtils.isNotBlank(val)) {
                start = Boolean.valueOf(val);
            }
        }
        this.setPoolSize(DEFAULT_POOL_SIZE);
        if (ConsoleManager.properties.containsKey(ZKManager.KEYS.poolSize.key)) {
            String val = String.valueOf(ConsoleManager.properties.get(ZKManager.KEYS.poolSize.key));
            if (StringUtils.isNotBlank(val)) {
                this.setPoolSize(Integer.valueOf(val));
            }
        }
        logger.info("SchedulerTaskManager properties: " + ConsoleManager.properties);
        this.init(ConsoleManager.properties);
    }

    /**
     * 调度器核心管理 初始化zookeeper链接
     *
     * @throws Exception 异常信息
     */
    public void init(Properties p) throws Exception {
        this.initLock.lock();
        try {
            this.scheduleTask = null;
            this.schedulerServer = null;
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.zkManager = new ZKManager(p);
            String errorMessage;
            int count = 0;
            while (!this.zkManager.checkZookeeperState()) {
                count = count + 1;
                if (count % 50 == 0) {
                    errorMessage = "Zookeeper connecting ......"
                            + this.zkManager.getConnectStr() + " spendTime:"
                            + count * 20 + "(ms)";
                    LOGGER.error(errorMessage);
                }
                Thread.sleep(20);
            }
            // zookeeper成功链接以后
            initialData();
        } finally {
            this.initLock.unlock();
        }
    }

    /**
     * 在Zk状态正常后回调数据初始化
     */
    public void initialData() throws Exception {
        this.pathTask = this.zkManager.getRootPath() + "/" + NODE_TASK;
        this.pathServer = this.zkManager.getRootPath() + "/" + NODE_SERVER;
        // 创建父节点 判断父节点是否可用
        this.zkManager.initial();
        this.initPathAndWatch(this.pathServer);
        this.initPathAndWatch(this.pathTask);
        this.scheduleTask = new ScheduleTaskForZookeeper(this.zkManager, this.pathTask);
        this.schedulerServer = new SchedulerServerForZookeeper(this.zkManager, this.pathServer, this.pathTask);
        if (this.start) {
            // 注册调度管理器
            this.schedulerServer.registerScheduleServer(this.currenScheduleServer);
        }
    }

    /**
     * 初始化并且监听节点
     */
    private void initPathAndWatch(String path) {
        try {
            if (this.zkManager.getZooKeeper().exists(path, false) == null) {
                ZKTools.createPath(this.zkManager.getZooKeeper(), path, CreateMode.PERSISTENT, this.zkManager.getAcl());
            }
            this.zkManager.getZooKeeper().getChildren(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    LOGGER.info("节点变更事件: " + event.getType() + "==========" + event.getPath());
                    switch (event.getType()) {
                        case None:
                        case NodeCreated:
                        case NodeDeleted:
                        case NodeDataChanged:
                        case NodeChildrenChanged:
                        default:
                            refreshScheduleServerAndTasks();
                            LOGGER.info("继续下一次监听");
                            initPathAndWatch(event.getPath());
                            break;
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error("initPathServer failed", e);
        }
    }

    /**
     * 1. 重新分配任务
     * 2. 检查本地任务
     */
    public void refreshScheduleServerAndTasks() {
        // 重新分配任务
        this.assignScheduleTask();
        // 检查本地任务
        this.checkLocalTask();
    }

    /**
     * 根据当前调度服务器的信息，重新计算分配所有的调度任务
     * 任务的分配是需要加锁，避免数据分配错误。为了避免数据锁带来的负面作用，通过版本号来达到锁的目的
     * <p>
     * 1、获取任务状态的版本号 2、获取所有的服务器注册信息和任务队列信息 3、清除已经超过心跳周期的服务器注册信息 3、重新计算任务分配
     * 4、更新任务状态的版本号【乐观锁】 5、根系任务队列的分配信息
     */
    public void assignScheduleTask() {
        List<String> serverList = schedulerServer.loadScheduleServerNames();
        //黑名单
        for (String ip : zkManager.getIpBlacklist()) {
            int index = serverList.indexOf(ip);
            if (index > -1) {
                serverList.remove(index);
            }
        }
        // 设置初始化成功标准，避免在leader转换的时候，新增的线程组初始化失败
        schedulerServer.assignTask(this.currenScheduleServer.getUuid(), serverList);
    }

    /**
     * 检查本地任务
     */
    public void checkLocalTask() {
        schedulerServer.checkLocalTask(this.currenScheduleServer.getUuid());
    }

    private Runnable taskWrapper(final Runnable task) {
        return new Runnable() {
            @Override
            public void run() {
                TaskDefine taskDefine = resolveTaskName(task);
                String name = taskDefine.stringKey();
                if (StringUtils.isNotEmpty(name)) {
                    boolean isOwner = false;
                    boolean isRunning = true;
                    try {
                        if (!isScheduleServerRegister) {
                            Thread.sleep(1000);
                        }
                        if (zkManager.checkZookeeperState()) {
                            isOwner = schedulerServer.isOwner(name, currenScheduleServer.getUuid());
                            isOwnerMap.put(name, isOwner);
                            isRunning = scheduleTask.isRunning(name);
                        } else {
                            // 如果zk不可用，使用历史数据
                            if (null != isOwnerMap) {
                                isOwner = isOwnerMap.get(name);
                            }
                        }
                        if (isOwner && isRunning) {
                            String msg = null;
                            try {
                                task.run();
                                LOGGER.info("任务[" + name + "] 成功触发!");
                            } catch (Exception e) {
                                msg = e.getLocalizedMessage();
                            }
                            scheduleTask.saveRunningInfo(name, currenScheduleServer.getUuid(), msg);
                        } else {
                            if (!isOwner) {
                                LOGGER.info("任务[" + name + "] 触发失败, 不属于当前server["+ currenScheduleServer.getUuid() +"]");
                            }
                            if (!isRunning) {
                                LOGGER.info("任务[" + name + "] 触发失败, 任务被暂停了");
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("Check task owner error.", e);
                    }
                }
            }
        };
    }

    private TaskDefine resolveTaskName(final Runnable task) {
        Method targetMethod;
        TaskDefine taskDefine = new TaskDefine();
        if (task instanceof ScheduledMethodRunnable) {
            ScheduledMethodRunnable uncodeScheduledMethodRunnable = (ScheduledMethodRunnable) task;
            targetMethod = uncodeScheduledMethodRunnable.getMethod();
            taskDefine.setType(TaskDefine.TYPE_UNCODE_TASK);
            if (StringUtils.isNotBlank(uncodeScheduledMethodRunnable.getExtKeySuffix())) {
                taskDefine.setExtKeySuffix(uncodeScheduledMethodRunnable.getExtKeySuffix());
            }
        } else {
            org.springframework.scheduling.support.ScheduledMethodRunnable springScheduledMethodRunnable = (org.springframework.scheduling.support.ScheduledMethodRunnable) task;
            targetMethod = springScheduledMethodRunnable.getMethod();
            taskDefine.setType(TaskDefine.TYPE_SPRING_TASK);
        }
        String[] beanNames = applicationcontext.getBeanNamesForType(targetMethod.getDeclaringClass());
        if (null != beanNames && StringUtils.isNotEmpty(beanNames[0])) {
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

    public void setZkManager(ZKManager zkManager) {
        this.zkManager = zkManager;
    }

    public ZKManager getZkManager() {
        return zkManager;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            taskDefine.setPeriod(period);
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.scheduleAtFixedRate(taskWrapper(task), period);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            String cronEx = trigger.toString();
            int index = cronEx.indexOf(":");
            if (index >= 0) {
                cronEx = cronEx.substring(index + 1);
                taskDefine.setCronExpression(cronEx.trim());
            }
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.schedule(taskWrapper(task), trigger);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            taskDefine.setStartTime(startTime);
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.schedule(taskWrapper(task), startTime);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            taskDefine.setStartTime(startTime);
            taskDefine.setPeriod(period);
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.scheduleAtFixedRate(taskWrapper(task), startTime, period);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            taskDefine.setStartTime(startTime);
            taskDefine.setPeriod(delay);
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(task), startTime, delay);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
        ScheduledFuture scheduledFuture = null;
        try {
            TaskDefine taskDefine = resolveTaskName(task);
            taskDefine.setPeriod(delay);
            scheduleTask.addTask(taskDefine);

            scheduledFuture = super.scheduleWithFixedDelay(taskWrapper(task), delay);
            DynamicTaskManager.checkTask(taskDefine, scheduledFuture);

            LOGGER.debug(currenScheduleServer.getUuid() + ":自动向集群注册任务[" + taskDefine.stringKey() + "]");
        } catch (Exception e) {
            LOGGER.error("update task error", e);
        }
        return scheduledFuture;
    }

    public boolean checkAdminUser(String account, String password) {
        if (StringUtils.isBlank(account) || StringUtils.isBlank(password)) {
            return false;
        }
        String name = this.config.get(ZKManager.KEYS.userName.key);
        String pwd = this.config.get(ZKManager.KEYS.password.key);
        return account.equals(name) && password.equals(pwd);
    }

    public String getScheduleServerUUid() {
        if (null != currenScheduleServer) {
            return currenScheduleServer.getUuid();
        }
        return null;
    }

    public Map<String, Boolean> getIsOwnerMap() {
        return isOwnerMap;
    }

    public static ApplicationContext getApplicationcontext() {
        return SchedulerTaskManager.applicationcontext;
    }

    @Override
    public void destroy() {
        if (this.zkManager != null) {
            try {
                this.zkManager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String getPathServer() {
        return pathServer;
    }

    public String getPathTask() {
        return pathTask;
    }
}