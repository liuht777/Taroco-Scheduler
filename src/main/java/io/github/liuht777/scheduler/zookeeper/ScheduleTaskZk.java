package io.github.liuht777.scheduler.zookeeper;

import io.github.liuht777.scheduler.constant.DefaultConstants;
import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.List;

import static io.github.liuht777.scheduler.constant.DefaultConstants.STATUS_ERROR;

/**
 * 定义任务task的zookeeper实现
 *
 * @author liuht
 */
@Slf4j
public class ScheduleTaskZk implements IScheduleTask {
    private CuratorFramework client;
    private String pathTask;

    public ScheduleTaskZk(String pathTask) {
        this.pathTask = pathTask;
    }

    @Override
    public void setClient(final CuratorFramework client) {
        this.client = client;
    }

    @Override
    public boolean isRunning(String name) {
        boolean isRunning = true;
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + name;
        //是否手动停止
        try {
            if (this.client.checkExists().forPath(zkPath) != null) {
                byte[] data = this.client.getData().forPath(zkPath);
                if (null != data) {
                    String json = new String(data);
                    Task task = JsonUtil.json2Object(json, Task.class);
                    assert task != null;
                    if (task.isStop()) {
                        isRunning = false;
                    }
                }
            } else {
                isRunning = false;
            }
        } catch (Exception e) {
            log.error("zookeeper error", e);
        }
        return isRunning;
    }

    @Override
    public boolean saveRunningInfo(String name, String uuid) {
        return saveRunningInfo(name, uuid, null);
    }

    @Override
    public boolean saveRunningInfo(String name, String uuid, String errorMsg) {
        //查看集群中是否注册当前任务，如果没有就自动注册
        String zkPath = this.pathTask + "/" + name;
        //判断是否分配给当前节点
        zkPath = zkPath + "/" + uuid;
        try {
            if (this.client.checkExists().forPath(zkPath) != null) {
                int times = 0;
                byte[] dataVal = this.client.getData().forPath(zkPath);
                if (dataVal != null) {
                    String val = new String(dataVal);
                    String[] vals = val.split(":");
                    times = Integer.parseInt(vals[0]);
                }
                times++;
                String newVal;
                if (StringUtils.isNotBlank(errorMsg)) {
                    newVal = "0:" + System.currentTimeMillis() + ":" + errorMsg;
                } else {
                    newVal = times + ":" + System.currentTimeMillis();
                }
                this.client.setData().forPath(zkPath, newVal.getBytes());
            }
        } catch (Exception e) {
            log.error("zookeeper error", e);
        }
        return true;
    }

    @Override
    public boolean isExistsTask(Task task) {
        String zkPath = this.pathTask + "/" + task.stringKey();
        if (this.client == null) {
            return true;
        }
        try {
            return this.client.checkExists().forPath(zkPath) != null;
        } catch (Exception e) {
            log.error("zookeeper error", e);
        }
        return false;
    }

    @Override
    public void addTask(Task task) {
        try {
            String zkPath = this.pathTask;
            zkPath = zkPath + "/" + task.stringKey();
            if (StringUtils.isBlank(task.getType())) {
                task.setType(DefaultConstants.TYPE_TAROCO_TASK);
            }
            String json = JsonUtil.object2Json(task);
            if (this.client.checkExists().forPath(zkPath) == null) {
                this.client.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, json.getBytes());
            }
        } catch (Exception e) {
            log.error("addTask failed:", e);
        }
    }

    @Override
    public void updateTask(Task task) {
        String zkPath = this.pathTask;
        zkPath = zkPath + "/" + task.stringKey();
        try {
            if (this.client.checkExists().forPath(zkPath) != null) {
                byte[] data = this.client.getData().forPath(zkPath);
                Task tmpTask = JsonUtil.json2Object(new String(data), Task.class);
                assert tmpTask != null;
                tmpTask.valueOf(task);
                String json = JsonUtil.object2Json(tmpTask);
                log.info("更新任务: {}", json);
                this.client.setData().forPath(zkPath, json.getBytes());
            }
        } catch (Exception e) {
            log.error("updateTask failed:", e);
        }
    }

    @Override
    public void delTask(Task task) {
        try {
            String zkPath = this.pathTask;
            if (this.client.checkExists().forPath(zkPath) != null) {
                zkPath = zkPath + "/" + task.stringKey();
                if (this.client.checkExists().forPath(zkPath) != null) {
                    log.info("删除任务: {}", zkPath);
                    this.client.delete().deletingChildrenIfNeeded().forPath(zkPath);
                }
            }
        } catch (Exception e) {
            log.error("delTask failed:", e);
        }
    }

    @Override
    public List<Task> selectTask() {
        String zkPath = this.pathTask;
        List<Task> tasks = new ArrayList<>();
        try {
            if (this.client.checkExists().forPath(zkPath) != null) {
                List<String> childes = this.client.getChildren().forPath(zkPath);
                for (String child : childes) {
                    byte[] data = this.client.getData().forPath(zkPath + "/" + child);
                    Task task;
                    if (null != data) {
                        String json = new String(data);
                        task = JsonUtil.json2Object(json, Task.class);
                    } else {
                        task = new Task();
                    }
                    String[] names = child.split("#");
                    assert task != null;
                    if (StringUtils.isNotEmpty(names[0])) {
                        task.setTargetBean(names[0]);
                        task.setTargetMethod(names[1]);
                    }
                    List<String> sers = this.client.getChildren().forPath(zkPath + "/" + child);
                    if (sers != null && sers.size() > 0) {
                        task.setCurrentServer(sers.get(0));
                        byte[] dataVal = this.client.getData().forPath(zkPath + "/" + child + "/" + sers.get(0));
                        if (dataVal != null) {
                            String val = new String(dataVal);
                            String[] vals = val.split(":");
                            task.setRunTimes(Integer.valueOf(vals[0]));
                            task.setLastRunningTime(Long.valueOf(vals[1]));
                            if (vals.length > 2 && StringUtils.isNotBlank(vals[2])) {
                                task.setStatus(STATUS_ERROR);
                                task.setErrorMsg(vals[2]);
                            }
                        }
                    }
                    tasks.add(task);
                }
            }
        } catch (Exception e) {
            log.error("ZK error", e);
        }
        return tasks;
    }

    @Override
    public Task selectTask(Task task) {
        String zkPath = this.pathTask + "/" + task.stringKey();
        try {
            byte[] data = this.client.getData().forPath(zkPath);
            if (null != data) {
                String json = new String(data);
                task = JsonUtil.json2Object(json, Task.class);
            } else {
                task = new Task();
            }
            List<String> sers = this.client.getChildren().forPath(zkPath);
            if (task != null && sers != null && sers.size() > 0) {
                task.setCurrentServer(sers.get(0));
                byte[] dataVal = this.client.getData().forPath(zkPath + "/" + sers.get(0));
                if (dataVal != null) {
                    String val = new String(dataVal);
                    String[] vals = val.split(":");
                    task.setRunTimes(Integer.valueOf(vals[0]));
                    task.setLastRunningTime(Long.valueOf(vals[1]));
                    if (vals.length > 2 && StringUtils.isNotBlank(vals[2])) {
                        task.setStatus(STATUS_ERROR + ":" + vals[2]);
                    }
                }
            }
        } catch (Exception e) {
            log.error("ZK error", e);
        }
        return task;
    }


}
