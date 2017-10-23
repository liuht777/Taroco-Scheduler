package cn.uncode.schedule.zk;

import cn.uncode.schedule.DynamicTaskManager;
import cn.uncode.schedule.core.ISchedulerServer;
import cn.uncode.schedule.core.ScheduleServer;
import cn.uncode.schedule.core.TaskDefine;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

/**
 * 任务器操作实现类
 *
 * @author liuht
 * @date 2017/10/21 14:27
 */
public class SchedulerServerForZookeeper implements ISchedulerServer {

    private static final transient Logger LOG = LoggerFactory.getLogger(SchedulerServerForZookeeper.class);
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private String pathServer;
    private String pathTask;
    private ZKManager zkManager;
    private long zkBaseTime = 0;
    private long loclaBaseTime = 0;

    public SchedulerServerForZookeeper(ZKManager zkManager, String pathServer, String pathTask) {
        this.zkManager = zkManager;
        this.pathTask = pathTask;
        this.pathServer = pathServer;
        try {
            long timeApart = 5000;
            // zookeeper时间与服务端时间差距判断
            String tempPath = this.zkManager.getZooKeeper().create(this.zkManager.getRootPath() + "/systime", null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
            Stat tempStat = this.zkManager.getZooKeeper().exists(tempPath, false);
            zkBaseTime = tempStat.getCtime();
            ZKTools.deleteTree(this.zkManager.getZooKeeper(), tempPath);
            loclaBaseTime = System.currentTimeMillis();
            if (Math.abs(this.zkBaseTime - this.loclaBaseTime) > timeApart) {
                LOG.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) + " ms");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void registerScheduleServer(ScheduleServer server) {
        try {
            if (server.isRegister()) {
                LOG.error(server.getUuid() + " 被重复注册");
                return;
            }
            String realPath;
            //此处必须增加UUID作为唯一性保障
            String id = server.getIp() + "$" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
            String zkServerPath = pathServer + "/" + id + "$";
            // 永久节点
            realPath = this.zkManager.getZooKeeper().create(zkServerPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
            server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));

            Timestamp heartBeatTime = new Timestamp(getSystemTime());
            server.setHeartBeatTime(heartBeatTime);

            String valueString = this.gson.toJson(server);
            this.zkManager.getZooKeeper().setData(realPath, valueString.getBytes(), -1);
            server.setRegister(true);
        } catch (Exception e) {
            LOG.error("registerScheduleServer failed:", e);
        }
    }

    @Override
    public void unRegisterScheduleServer(ScheduleServer server) {
        List<String> serverList = this.loadScheduleServerNames();
        try {
            if (server.isRegister() && this.isLeader(server.getUuid(), serverList)) {
                //delete task
                String zkPath = this.pathTask;
                String serverPath = this.pathServer;
                if (this.zkManager.getZooKeeper().exists(zkPath, false) == null) {
                    this.zkManager.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
                }
                //get all task
                List<String> children = this.zkManager.getZooKeeper().getChildren(zkPath, false);
                if (null != children && children.size() > 0) {
                    for (String taskName : children) {
                        String taskPath = zkPath + "/" + taskName;
                        if (this.zkManager.getZooKeeper().exists(taskPath, false) != null) {
                            ZKTools.deleteTree(this.zkManager.getZooKeeper(), taskPath + "/" + server.getUuid());
                        }
                    }
                }
                //删除
                if (this.zkManager.getZooKeeper().exists(this.pathServer, false) == null) {
                    ZKTools.deleteTree(this.zkManager.getZooKeeper(), serverPath + serverPath + "/" + server.getUuid());
                }
                server.setRegister(false);
            }
        } catch (Exception e) {
            LOG.error("unRegisterScheduleServer failed", e);
        }
    }

    @Override
    public List<String> loadScheduleServerNames() {
        List<String> serverList = new ArrayList<>(1);
        try {
            String zkPath = this.pathServer;
            if (this.zkManager.getZooKeeper().exists(zkPath, false) == null) {
                return new ArrayList<>();
            }
            serverList = this.zkManager.getZooKeeper().getChildren(zkPath, false);
            serverList.sort(Comparator.comparing(u -> u.substring(u.lastIndexOf("$") + 1)));
        } catch (Exception e) {
            LOG.error("loadScheduleServerNames failed", e);
        }
        return serverList;
    }

    @Override
    public void assignTask(String currentUuid, List<String> taskServerList) {
        LOG.info("当前server:["+ currentUuid + "]:开始重新分配任务......");
        if (!this.isLeader(currentUuid, taskServerList)) {
            LOG.info("当前server:["+ currentUuid + "]:不是负责任务分配的Leader,直接返回");
            return;
        }
        if (taskServerList.size() <= 0) {
            //在服务器动态调整的时候，可能出现服务器列表为空的清空
            LOG.info("服务器列表为空: 停止分配任务, 等待服务器上线...");
            return;
        }
        try {
            if (this.zkManager.checkZookeeperState()) {
                String zkPath = this.pathTask;
                if (this.zkManager.getZooKeeper().exists(zkPath, false) == null) {
                    this.zkManager.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
                }
                List<String> children = this.zkManager.getZooKeeper().getChildren(zkPath, false);
                if (null != children && children.size() > 0) {
                    for (String taskName : children) {
                        String taskPath = zkPath + "/" + taskName;
                        if (this.zkManager.getZooKeeper().exists(taskPath, false) == null) {
                            this.zkManager.getZooKeeper().create(taskPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
                        }

                        List<String> taskServerIds = this.zkManager.getZooKeeper().getChildren(taskPath, false);
                        if (null == taskServerIds || taskServerIds.size() == 0) {
                            assignServer2Task(taskServerList, taskPath);
                        } else {
                            boolean hasAssignSuccess = false;
                            for (String serverId : taskServerIds) {
                                if (taskServerList.contains(serverId)) {
                                    //防止重复分配任务，如果已经成功分配，第二个以后都删除
                                    if (hasAssignSuccess) {
                                        ZKTools.deleteTree(this.zkManager.getZooKeeper(), taskPath + "/" + serverId);
                                    } else {
                                        hasAssignSuccess = true;
                                        continue;
                                    }
                                }
                                ZKTools.deleteTree(this.zkManager.getZooKeeper(), taskPath + "/" + serverId);
                            }
                            if (!hasAssignSuccess) {
                                assignServer2Task(taskServerList, taskPath);
                            }
                        }

                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(currentUuid + ":没有集群任务");
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("assignTask failed:", e);
        }
    }

    private void assignServer2Task(List<String> taskServerList, String taskPath) throws Exception {
        Random random = new Random();
        int index = random.nextInt(taskServerList.size());
        String serverId = taskServerList.get(index);
        try {
            if (this.zkManager.getZooKeeper().exists(taskPath, false) != null) {
                this.zkManager.getZooKeeper().create(taskPath + "/" + serverId, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);

                LOG.info("成功分配任务 [" + taskPath + "]" + " 给 server [" + serverId + "]");
            }
        } catch (Exception e) {
            LOG.error("assign task error", e);
        }
    }

    @Override
    public void checkLocalTask(String currentUuid) {
        try {
            if (this.zkManager.checkZookeeperState()) {
                String zkPath = this.pathTask;
                List<String> children = this.zkManager.getZooKeeper().getChildren(zkPath, false);
                List<String> ownerTask = new ArrayList<String>();
                if (null != children && children.size() > 0) {
                    for (String taskName : children) {
                        if (isOwner(taskName, currentUuid)) {
                            String taskPath = zkPath + "/" + taskName;
                            byte[] data = this.zkManager.getZooKeeper().getData(taskPath, null, null);
                            if (null != data) {
                                String json = new String(data);
                                TaskDefine td = this.gson.fromJson(json, TaskDefine.class);
                                TaskDefine taskDefine = new TaskDefine();
                                taskDefine.valueOf(td);
                                ownerTask.add(taskName);
                                if (TaskDefine.TYPE_UNCODE_TASK.equals(taskDefine.getType())) {
                                    // 动态任务才使用 DynamicTaskManager启动
                                    DynamicTaskManager.scheduleTask(taskDefine, new Date(getSystemTime()));
                                }
                            }
                        }
                    }
                }
                DynamicTaskManager.clearLocalTask(ownerTask);
            }
        } catch (Exception e) {
            LOG.error("checkLocalTask failed", e);
        }
    }

    @Override
    public boolean isOwner(String name, String uuid) {
        boolean isOwner = false;
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + name;
        //判断是否分配给当前节点
        try {
            if (this.zkManager.getZooKeeper().exists(zkPath + "/" + uuid, false) != null) {
                isOwner = true;
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("ZK error", e);
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
