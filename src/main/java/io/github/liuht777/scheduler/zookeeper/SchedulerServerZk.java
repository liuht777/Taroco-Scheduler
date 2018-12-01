package io.github.liuht777.scheduler.zookeeper;

import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.ScheduleServer;
import io.github.liuht777.scheduler.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.*;

/**
 * server节点接口的zookeeper实现
 *
 * @author liuht
 * @date 2017/10/21 14:27
 */
@Slf4j
public class SchedulerServerZk implements ISchedulerServer {

    private String pathServer;
    private String pathTask;
    private CuratorFramework client;

    public SchedulerServerZk(String pathServer, String pathTask) {
        this.pathTask = pathTask;
        this.pathServer = pathServer;
    }

    @Override
    public void setClient(final CuratorFramework client) {
        this.client = client;
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
            realPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(zkServerPath);
            server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));
            String valueString = JsonUtil.object2Json(server);
            client.setData().forPath(realPath, valueString.getBytes());
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
            if (client.checkExists().forPath(zkPath) == null) {
                return Collections.emptyList();
            }
            serverList = client.getChildren().forPath(zkPath);
            serverList.sort(Comparator.comparing(u -> u.substring(u.lastIndexOf("$") + 1)));
        } catch (Exception e) {
            log.error("loadScheduleServerNames failed", e);
        }
        return serverList;
    }
    @Override
    public boolean isOwner(String taskName, String serverUuid) {
        boolean isOwner = false;
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + taskName;
        //判断是否分配给当前节点
        try {
            if (client.checkExists().forPath(zkPath + "/" + serverUuid) != null) {
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
}
