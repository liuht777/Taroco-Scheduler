package cn.taroco.scheduler.zk;

import cn.taroco.scheduler.core.IScheduleTask;
import cn.taroco.scheduler.core.TaskDefine;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 定时任务操作实现类
 *
 * @author juny.ye
 */
public class ScheduleTaskForZookeeper implements IScheduleTask {
    private static final transient Logger LOG = LoggerFactory.getLogger(ScheduleTaskForZookeeper.class);

    private Gson gson = new GsonBuilder().create();
    private ZKManager zkManager;
    private String pathTask;

    public ScheduleTaskForZookeeper(ZKManager zkManager, String pathTask) throws Exception {
        this.zkManager = zkManager;
        this.pathTask = pathTask;
    }

    public ZooKeeper getZooKeeper() {
        return this.zkManager.getZooKeeper();
    }

    @Override
    public boolean isRunning(String name) {
        boolean isRunning = true;
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + name;
        //是否手动停止
        try {
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                byte[] data = this.getZooKeeper().getData(zkPath, null, null);
                if (null != data) {
                    String json = new String(data);
                    TaskDefine taskDefine = this.gson.fromJson(json, TaskDefine.class);
                    if (taskDefine.isStop()) {
                        isRunning = false;
                    }
                }
            } else {
                isRunning = false;
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("zk error", e);
        }
        return isRunning;
    }

    @Override
    public boolean saveRunningInfo(String name, String uuid) {
        return saveRunningInfo(name, uuid, null);
    }

    @Override
    public boolean saveRunningInfo(String name, String uuid, String msg) {
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + name;
        //判断是否分配给当前节点
        zkPath = zkPath + "/" + uuid;
        try {
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                int times = 0;
                byte[] dataVal = this.getZooKeeper().getData(zkPath, null, null);
                if (dataVal != null) {
                    String val = new String(dataVal);
                    String[] vals = val.split(":");
                    times = Integer.parseInt(vals[0]);
                }
                times++;
                String newVal;
                if (StringUtils.isNotBlank(msg)) {
                    newVal = "0:" + System.currentTimeMillis() + ":" + msg;
                } else {
                    newVal = times + ":" + System.currentTimeMillis();
                }
                this.getZooKeeper().setData(zkPath, newVal.getBytes(), -1);
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("zk error", e);
        }
        return true;
    }

    @Override
    public boolean isExistsTask(TaskDefine taskDefine) {
        String zkPath = this.pathTask + "/" + taskDefine.stringKey();
        try {
            return this.getZooKeeper().exists(zkPath, false) != null;
        } catch (KeeperException | InterruptedException e) {
            LOG.error("zk error", e);
        }
        return false;
    }

    @Override
    public void addTask(TaskDefine taskDefine) {
        try {
            String zkPath = this.pathTask;
            zkPath = zkPath + "/" + taskDefine.stringKey();
            if (this.getZooKeeper().exists(zkPath, false) == null) {
                this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
            }
            byte[] data = this.getZooKeeper().getData(zkPath, null, null);
            if (null == data || data.length == 0) {
                if (StringUtils.isBlank(taskDefine.getType())) {
                    taskDefine.setType(TaskDefine.TYPE_UNCODE_TASK);
                }
                String json = this.gson.toJson(taskDefine);
                this.getZooKeeper().setData(zkPath, json.getBytes(), -1);
            }
        } catch (Exception e) {
            LOG.error("addTask failed:", e);
        }
    }

    @Override
    public void updateTask(TaskDefine taskDefine) {
        String zkPath = this.pathTask;
        zkPath = zkPath + "/" + taskDefine.stringKey();
        try {
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                byte[] data = this.getZooKeeper().getData(zkPath, null, null);
                TaskDefine tmpTaskDefine;
                if (null != data) {
                    String json = new String(data);
                    tmpTaskDefine = this.gson.fromJson(json, TaskDefine.class);
                    tmpTaskDefine.valueOf(tmpTaskDefine);
                } else {
                    tmpTaskDefine = new TaskDefine();
                }
                tmpTaskDefine.valueOf(taskDefine);
                String json = this.gson.toJson(tmpTaskDefine);
                this.getZooKeeper().setData(zkPath, json.getBytes(), -1);
            }
        } catch (Exception e) {
            LOG.error("updateTask failed:", e);
        }
    }

    @Override
    public void delTask(TaskDefine taskDefine) {
        try {
            String zkPath = this.pathTask;
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                zkPath = zkPath + "/" + taskDefine.stringKey();
                if (this.getZooKeeper().exists(zkPath, false) != null) {
                    ZKTools.deleteTree(this.getZooKeeper(), zkPath);
                }
            }
        } catch (Exception e) {
            LOG.error("delTask failed:", e);
        }
    }

    @Override
    public List<TaskDefine> selectTask() {
        String zkPath = this.pathTask;
        List<TaskDefine> taskDefines = new ArrayList<>();
        try {
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                List<String> childes = this.getZooKeeper().getChildren(zkPath, false);
                for (String child : childes) {
                    byte[] data = this.getZooKeeper().getData(zkPath + "/" + child, null, null);
                    TaskDefine taskDefine = null;
                    if (null != data) {
                        String json = new String(data);
                        taskDefine = this.gson.fromJson(json, TaskDefine.class);
                    } else {
                        taskDefine = new TaskDefine();
                    }
                    String[] names = child.split("#");
                    if (StringUtils.isNotEmpty(names[0])) {
                        taskDefine.setTargetBean(names[0]);
                        taskDefine.setTargetMethod(names[1]);
                    }
                    List<String> sers = this.getZooKeeper().getChildren(zkPath + "/" + child, false);
                    if (taskDefine != null && sers != null && sers.size() > 0) {
                        taskDefine.setCurrentServer(sers.get(0));
                        byte[] dataVal = this.getZooKeeper().getData(zkPath + "/" + child + "/" + sers.get(0), null, null);
                        if (dataVal != null) {
                            String val = new String(dataVal);
                            String[] vals = val.split(":");
                            taskDefine.setRunTimes(Integer.valueOf(vals[0]));
                            taskDefine.setLastRunningTime(Long.valueOf(vals[1]));
                            if (vals.length > 2 && StringUtils.isNotBlank(vals[2])) {
                                taskDefine.setStatus(TaskDefine.STATUS_ERROR + ":" + vals[2]);
                            }
                        }
                    }
                    taskDefines.add(taskDefine);
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("ZK error", e);
        }
        return taskDefines;
    }

    @Override
    public TaskDefine selectTask(TaskDefine taskDefine) {
        String zkPath = this.pathTask + "/" + taskDefine.stringKey();
        try {
            byte[] data = this.getZooKeeper().getData(zkPath, null, null);
            if (null != data) {
                String json = new String(data);
                taskDefine = this.gson.fromJson(json, TaskDefine.class);
            } else {
                taskDefine = new TaskDefine();
            }
            List<String> sers = this.getZooKeeper().getChildren(zkPath, false);
            if (taskDefine != null && sers != null && sers.size() > 0) {
                taskDefine.setCurrentServer(sers.get(0));
                byte[] dataVal = this.getZooKeeper().getData(zkPath + "/" + sers.get(0), null, null);
                if (dataVal != null) {
                    String val = new String(dataVal);
                    String[] vals = val.split(":");
                    taskDefine.setRunTimes(Integer.valueOf(vals[0]));
                    taskDefine.setLastRunningTime(Long.valueOf(vals[1]));
                    if (vals.length > 2 && StringUtils.isNotBlank(vals[2])) {
                        taskDefine.setStatus(TaskDefine.STATUS_ERROR + ":" + vals[2]);
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("ZK error", e);
        }
        return taskDefine;
    }


}
