package io.github.liuht777.scheduler.zookeeper;

import io.github.liuht777.scheduler.DynamicTaskManager;
import io.github.liuht777.scheduler.constant.DefaultConstants;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务器操作实现类
 *
 * @author liuht
 * @date 2017/10/21 14:27
 */
@Slf4j
public class SchedulerServer implements ISchedulerServer {

    private AtomicInteger pos = new AtomicInteger(0);
    private String pathServer;
    private String pathTask;
    private ZkClient zkClient;
    private long zkBaseTime = 0;
    private long loclaBaseTime = 0;

    public SchedulerServer(ZkClient zkClient, String pathServer, String pathTask) {
        this.zkClient = zkClient;
        this.pathTask = pathTask;
        this.pathServer = pathServer;
        try {
            long timeApart = 5000;
            // zookeeper时间与服务端时间差距判断
            final String rootPath = this.zkClient.getSchedulerProperties().getZk().getRootPath();
            final String tempPath = this.zkClient.getClient().create()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(rootPath + "/systime");
            final Stat tempStat = this.zkClient.getClient().checkExists().forPath(tempPath);
            zkBaseTime = tempStat.getCtime();
            this.zkClient.getClient().delete().deletingChildrenIfNeeded().forPath(tempPath);
            loclaBaseTime = System.currentTimeMillis();
            if (Math.abs(this.zkBaseTime - this.loclaBaseTime) > timeApart) {
                log.warn("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) + " ms");
            }
        } catch (Exception e) {
            log.error("zookeeper时间与本地时间校验失败.", e);
        }
    }

    @Override
    public void registerScheduleServer(ScheduleServer server) {
        try {
            if (server.isRegister()) {
                log.warn(server.getUuid() + " 被重复注册");
                return;
            }
            String realPath;
            //此处必须增加UUID作为唯一性保障
            final String id = server.getIp() + "$" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
            final String zkServerPath = pathServer + "/" + id + "$";
            // 临时顺序节点
            realPath = this.zkClient.getClient().create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(zkServerPath);
            server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));
            String valueString = JsonUtil.object2Json(server);
            this.zkClient.getClient().setData().forPath(realPath, valueString.getBytes());
            server.setRegister(true);
            log.info("注册server成功: {}", server.getUuid());
        } catch (Exception e) {
            log.error("registerScheduleServer failed:", e);
        }
    }

    @Override
    public List<String> loadScheduleServerNames() {
        List<String> serverList = new ArrayList<>(1);
        try {
            String zkPath = this.pathServer;
            if (this.zkClient.getClient().checkExists().forPath(zkPath) == null) {
                return Collections.emptyList();
            }
            serverList = this.zkClient.getClient().getChildren().forPath(zkPath);
            serverList.sort(Comparator.comparing(u -> u.substring(u.lastIndexOf("$") + 1)));
        } catch (Exception e) {
            log.error("loadScheduleServerNames failed", e);
        }
        return serverList;
    }

    @Override
    public void assignTask(String currentUuid, List<String> taskServerList) {
        log.info("当前server:[" + currentUuid + "]: 开始重新分配任务......");
        if (!this.isLeader(currentUuid, taskServerList)) {
            log.info("当前server:[" + currentUuid + "]: 不是负责任务分配的Leader,直接返回");
            return;
        }
        if (CollectionUtils.isEmpty(taskServerList)) {
            //在服务器动态调整的时候，可能出现服务器列表为空的情况
            log.info("服务器列表为空: 停止分配任务, 等待服务器上线...");
            return;
        }
        try {
            String zkPath = this.pathTask;
            List<String> taskNames = this.zkClient.getClient().getChildren().forPath(zkPath);
            if (CollectionUtils.isEmpty(taskNames)) {
                log.info("当前server:[" + currentUuid + "]: 分配结束,没有集群任务");
                return;
            }
            for (String taskName : taskNames) {
                String taskPath = zkPath + "/" + taskName;
                List<String> taskServerIds = this.zkClient.getClient().getChildren().forPath(taskPath);
                if (CollectionUtils.isEmpty(taskServerIds)) {
                    // 没有找到目标server信息, 执行分配任务给server节点
                    assignServer2Task(taskServerList, taskPath);
                } else {
                    boolean hasAssignSuccess = false;
                    for (String serverId : taskServerIds) {
                        if (taskServerList.contains(serverId)) {
                            //防止重复分配任务，如果已经成功分配，第二个以后都删除
                            if (hasAssignSuccess) {
                                this.zkClient.getClient().delete().deletingChildrenIfNeeded()
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
            if (this.zkClient.getClient().checkExists().forPath(taskPath) != null) {
                final String runningInfo = "0:" + System.currentTimeMillis();
                final String path = taskPath + "/" + serverId;
                final Stat stat = this.zkClient.getClient().checkExists().forPath(path);
                if (stat == null) {
                    this.zkClient.getClient()
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

    @Override
    public void checkLocalTask(String currentUuid) {
        try {
            String zkPath = this.pathTask;
            List<String> taskNames = this.zkClient.getClient().getChildren().forPath(zkPath);
            if (CollectionUtils.isEmpty(taskNames)) {
                log.info("当前server:[" + currentUuid + "]: 检查本地任务结束, 任务列表为空");
                return;
            }
            List<String> localTasks = new ArrayList<>();
            for (String taskName : taskNames) {
                if (isOwner(taskName, currentUuid)) {
                    String taskPath = zkPath + "/" + taskName;
                    byte[] data = this.zkClient.getClient().getData().forPath(taskPath);
                    if (null != data) {
                        String json = new String(data);
                        Task td = JsonUtil.json2Object(json, Task.class);
                        Task task = new Task();
                        task.valueOf(td);
                        localTasks.add(taskName);
                        if (DefaultConstants.TYPE_TAROCO_TASK.equals(task.getType())) {
                            // 动态任务才使用 DynamicTaskManager启动
                            DynamicTaskManager.scheduleTask(task);
                        }
                    }
                }
            }
            DynamicTaskManager.clearLocalTask(localTasks);
        } catch (Exception e) {
            log.error("checkLocalTask failed", e);
        }
    }

    @Override
    public boolean isOwner(String taskName, String serverUuid) {
        boolean isOwner = false;
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + taskName;
        //判断是否分配给当前节点
        try {
            if (this.zkClient.getClient().checkExists().forPath(zkPath + "/" + serverUuid) != null) {
                isOwner = true;
            }
        } catch (Exception e) {
            log.error("isOwner assert error", e);
        }
        return isOwner;
    }

    @Override
    public boolean isLeader(String uuid, List<String> serverList) {
        return uuid.equals(getLeader(serverList));
    }

    /**
     * 取serverCode最小的服务器为leader。这种方法的好处是，
     * 由于serverCode是递增的，再新增服务器的时候，leader节点不会变化，比较稳定，算法又简单。
     */
    private String getLeader(List<String> serverList) {
        if (serverList == null || serverList.size() == 0) {
            return "";
        }
        long no = Long.MAX_VALUE;
        long tmpNo = -1;
        String leader = null;
        for (String server : serverList) {
            tmpNo = Long.parseLong(server.substring(server.lastIndexOf("$") + 1));
            if (no > tmpNo) {
                no = tmpNo;
                leader = server;
            }
        }
        return leader;
    }

    private long getSystemTime() {
        return this.zkBaseTime + (System.currentTimeMillis() - this.loclaBaseTime);
    }
}
