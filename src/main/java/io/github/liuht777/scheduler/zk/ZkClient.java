package io.github.liuht777.scheduler.zk;

import io.github.liuht777.scheduler.config.TarocoSchedulerProperties;
import io.github.liuht777.scheduler.core.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;


/**
 * zookeeper 客户端 管理类
 *
 * @author liuht
 */
@Slf4j
public class ZkClient {

    /**
     * zookeeper 客户端
     */
    private CuratorFramework client;

    /**
     * 配置properties
     */
    private TarocoSchedulerProperties schedulerProperties;

    public ZkClient(TarocoSchedulerProperties schedulerProperties) {
        this.schedulerProperties = schedulerProperties;
        this.connect();
        this.initRootPath();
    }

    /**
     * 简历zookeeper连接
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
                    break;
                default:
                    break;
            }
        });
        client.start();
    }

    /**
     * 初始化root节点
     */
    public void initRootPath() {
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
     * 返回 client 对象
     *
     * @return CuratorFramework
     */
    public CuratorFramework getClient() {
        return client;
    }

    public TarocoSchedulerProperties getSchedulerProperties() {
        return schedulerProperties;
    }
}
