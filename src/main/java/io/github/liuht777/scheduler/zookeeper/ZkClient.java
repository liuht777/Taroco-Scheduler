package io.github.liuht777.scheduler.zookeeper;

import io.github.liuht777.scheduler.ThreadPoolTaskGenerator;
import io.github.liuht777.scheduler.config.TarocoSchedulerProperties;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.Version;
import io.github.liuht777.scheduler.event.AssignScheduleTaskEvent;
import io.github.liuht777.scheduler.event.ServerNodeAddEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_SERVER;
import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_TASK;


/**
 * zookeeper 客户端 管理类
 *
 * @author liuht
 */
@Slf4j
public class ZkClient implements ApplicationEventPublisherAware {
    /**
     * zookeeper 客户端
     */
    private CuratorFramework client;
    /**
     * 配置properties
     */
    private TarocoSchedulerProperties schedulerProperties;
    /**
     * task 线程对象
     */
    private ThreadPoolTaskGenerator taskGenerator;
    /**
     * task节点
     */
    private String taskPath;
    /**
     * server节点
     */
    private String serverPath;
    /**
     * 事件发布器
     */
    private ApplicationEventPublisher eventPublisher;

    public ZkClient(TarocoSchedulerProperties schedulerProperties,
                    ThreadPoolTaskGenerator taskGenerator) {
        this.schedulerProperties = schedulerProperties;
        this.taskGenerator = taskGenerator;
        this.connect();
    }

    /**
     * 建立zookeeper连接
     */
    private void connect() {
        final int baseSleepTimeMs = 1000;
        final int maxRetries = 3;
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        client = CuratorFrameworkFactory.builder()
                .connectString(this.schedulerProperties.getZk().getUrl())
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(this.schedulerProperties.getZk().getSessionTimeout())
                .connectionTimeoutMs(this.schedulerProperties.getZk().getConnectionTimeout())
                .build();
        client.getConnectionStateListenable().addListener((curatorFramework, state) -> {
            switch (state) {
                case LOST:
                    // 挂起后重试超时，客户端认为与zk服务器的连接丢失
                    log.warn("lost connection with zookeeper");
                    // 重新建立连接
                    this.connect();
                    break;
                case CONNECTED:
                    // 成功建立连接
                    log.info("connected with zookeeper");
                    this.initPath();
                    this.initWatchAndRegistServer();
                    break;
                case RECONNECTED:
                    // 挂起或者丢失连接后重新连接
                    log.info("reconnected with zookeeper");
                    this.initWatchAndRegistServer();
                    break;
                default:
                    break;
            }
        });
        client.start();
    }

    /**
     * 初始化需要的节点
     */
    private void initPath() {
        this.initRootPath();
        final String rootPath = this.schedulerProperties.getZk().getRootPath();
        this.taskPath = rootPath + "/" + NODE_TASK;
        this.serverPath = rootPath + "/" + NODE_SERVER;
        this.initPath(this.taskPath);
        this.initPath(this.serverPath);
    }

    /**
     * 初始化监听和注册当前server节点
     */
    private void initWatchAndRegistServer() {
        // 设置client对象
        this.taskGenerator.getSchedulerServer().setClient(client);
        this.taskGenerator.getScheduleTask().setClient(client);
        // 监听节点, 负责重新分配任务
        this.watchPath(this.taskPath);
        this.watchPath(this.serverPath);
        // 设置为未注册
        ScheduleServer.getInstance().setRegister(false);
        // 注册当前server
        this.taskGenerator.getSchedulerServer().registerScheduleServer(ScheduleServer.getInstance());
    }

    /**
     * 初始化root节点
     */
    private void initRootPath() {
        //当zk状态正常后才能调用
        final String rootPath = this.schedulerProperties.getZk().getRootPath();
        try {
            if (client.checkExists().forPath(rootPath) == null) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT).forPath(rootPath, Version.getVersion().getBytes());
            } else {
                //先校验父亲节点，本身是否已经是schedule的目录
                byte[] value = this.client.getData().forPath(rootPath);
                if (value == null) {
                    client.setData().forPath(rootPath, Version.getVersion().getBytes());
                } else {
                    String dataVersion = new String(value);
                    if (!Version.isCompatible(dataVersion)) {
                        log.warn("TarocoScheduler程序版本:" + Version.getVersion() + " ,不匹配Zookeeper中的数据版本:" + dataVersion);
                    }
                    log.info("当前TarocoScheduler的程序版本:" + Version.getVersion() + ", Zookeeper中的数据版本: " + dataVersion);
                }
            }
        } catch (Exception e) {
            log.error("初始化 rootPath 失败.", e);
        }
    }

    /**
     * 初始化path
     */
    private void initPath(String path) {
        try {
            if (this.client.checkExists().forPath(path) == null) {
                this.client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (Exception e) {
            log.error("initPath failed", e);
        }
    }

    /**
     * 监听指定节点
     * <p>
     * 主要做重新分配任务使用
     *
     * @param path
     */
    private void watchPath(String path) {
        try {
            // 监听子节点变化情况
            final PathChildrenCache watcher = new PathChildrenCache(this.client, path, true);
            watcher.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            watcher.getListenable().addListener(
                    (client, event) -> {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                log.info("监听到节点变化: 新增path=: {}", event.getData().getPath());
                                // 新增server节点和task节点都需要触发重新分配任务事件
                                if (event.getData().getPath().startsWith(this.serverPath)) {
                                    // 新增server节点触发 ServerNodeAddEvent
                                    eventPublisher.publishEvent(new ServerNodeAddEvent(event.getData().getPath()));
                                } else {
                                    // 新增task节点需要触发重新分配任务事件
                                    eventPublisher.publishEvent(new AssignScheduleTaskEvent(event.getData().getPath()));
                                }
                                break;
                            case CHILD_REMOVED:
                                log.info("监听到节点变化: 删除path=: {}", event.getData().getPath());
                                // 删除task节点不需要发布重新分配任务事件
                                if (event.getData().getPath().startsWith(this.serverPath)) {
                                    eventPublisher.publishEvent(new AssignScheduleTaskEvent(event.getData().getPath()));
                                }
                                break;
                            default:
                                break;
                        }
                    }
            );
        } catch (Exception e) {
            log.error("watchPath failed", e);
        }
    }

    /**
     * 返回 ThreadPoolTaskGenerator
     *
     * @return ThreadPoolTaskGenerator
     */
    public ThreadPoolTaskGenerator getTaskGenerator() {
        return taskGenerator;
    }

    /**
     * 返回 CuratorFramework
     *
     * @return CuratorFramework
     */
    public CuratorFramework getClient() {
        return client;
    }

    /**
     * 返回 TarocoSchedulerProperties
     *
     * @return TarocoSchedulerProperties
     */
    public TarocoSchedulerProperties getSchedulerProperties() {
        return schedulerProperties;
    }

    /**
     * taskPath
     *
     * @return taskPath
     */
    public String getTaskPath() {
        return taskPath;
    }

    /**
     * serverPath
     *
     * @return serverPath
     */
    public String getServerPath() {
        return serverPath;
    }

    @Override
    public void setApplicationEventPublisher(final ApplicationEventPublisher applicationEventPublisher) {
        this.eventPublisher = applicationEventPublisher;
    }
}
