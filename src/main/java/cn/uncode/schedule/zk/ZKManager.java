package cn.uncode.schedule.zk;

import cn.uncode.schedule.core.Version;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * zookeeper 管理类
 *
 * @author juny.ye
 */
public class ZKManager {

    private static transient Logger log = LoggerFactory.getLogger(ZKManager.class);
    private ZooKeeper zk;
    private List<ACL> acl = new ArrayList<>();
    private Properties properties;

    public enum KEYS {
        /**
         * 属性keys
         */
        zkConnectString("uncode.schedule.zkConnect"),
        rootPath("uncode.schedule.rootPath"),
        userName("uncode.schedule.zkUsername"),
        password("uncode.schedule.zkPassword"),
        zkSessionTimeout("uncode.schedule.zkSessionTimeout"),
        autoRegisterTask("uncode.schedule.autoRegisterTask"),
        ipBlacklist("uncode.schedule.ipBlackLists"),
        poolSize("uncode.schedule.poolSize");
        public String key;

        KEYS(String key) {
            this.key = key;
        }

    }

    public ZKManager(Properties aProperties) throws Exception {
        this.properties = aProperties;
        this.connect();
    }

    /**
     * 重连zookeeper
     *
     * @throws Exception
     */
    private synchronized void reConnection() throws Exception {
        if (this.zk != null) {
            this.zk.close();
            this.zk = null;
            this.connect();
        }
    }

    private void connect() throws Exception {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        createZookeeper(connectionLatch);
        connectionLatch.await();
    }

    /**
     * 建立zookeeper链接
     *
     * @param connectionLatch 同步锁
     * @throws Exception zk异常
     */
    private void createZookeeper(final CountDownLatch connectionLatch) throws Exception {
        zk = new ZooKeeper(this.properties.getProperty(KEYS.zkConnectString.key),
                Integer.parseInt(this.properties.getProperty(KEYS.zkSessionTimeout.key)),
                event -> sessionEvent(connectionLatch, event));
        acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    /**
     * zookeeper 链接事件监听
     *
     * @param connectionLatch 同步锁
     * @param event           事件
     */
    private void sessionEvent(CountDownLatch connectionLatch, WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            log.info("收到ZK连接成功事件！");
            connectionLatch.countDown();
        } else if (event.getState() == KeeperState.Expired) {
            log.error("会话超时，等待重新建立ZK连接...");
            try {
                reConnection();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        } // Disconnected：Zookeeper会自动处理Disconnected状态重连
    }

    public void close() throws InterruptedException {
        log.info("关闭zookeeper连接");
        this.zk.close();
    }

    public String getRootPath() {
        return this.properties.getProperty(KEYS.rootPath.key);
    }

    public List<String> getIpBlacklist() {
        List<String> ips = new ArrayList<String>();
        String list = this.properties.getProperty(KEYS.ipBlacklist.key);
        if (StringUtils.isNotEmpty(list)) {
            ips = Arrays.asList(list.split(","));
        }
        return ips;
    }

    public String getConnectStr() {
        return this.properties.getProperty(KEYS.zkConnectString.key);
    }

    boolean isAutoRegisterTask() {
        String autoRegisterTask = this.properties.getProperty(KEYS.autoRegisterTask.key);
        if (StringUtils.isNotEmpty(autoRegisterTask)) {
            return Boolean.valueOf(autoRegisterTask);
        }
        return true;
    }

    public boolean checkZookeeperState() throws Exception {
        return zk != null && zk.getState() == States.CONNECTED;
    }

    /**
     * 创建父节点 判断父节点是否可用
     *
     * @throws Exception 异常信息
     */
    public void initial() throws Exception {
        //当zk状态正常后才能调用
        checkParent(zk, this.getRootPath());
        if (zk.exists(this.getRootPath(), false) == null) {
            ZKTools.createPath(zk, this.getRootPath(), CreateMode.PERSISTENT, acl);
            //设置版本信息
            zk.setData(this.getRootPath(), Version.getVersion().getBytes(), -1);
        } else {
            //先校验父亲节点，本身是否已经是schedule的目录
            byte[] value = zk.getData(this.getRootPath(), false, null);
            if (value == null) {
                zk.setData(this.getRootPath(), Version.getVersion().getBytes(), -1);
            } else {
                String dataVersion = new String(value);
                if (!Version.isCompatible(dataVersion)) {
                    throw new Exception("TBSchedule程序版本 " + Version.getVersion() + " 不兼容Zookeeper中的数据版本 " + dataVersion);
                }
                log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
            }
        }
    }

    private static void checkParent(ZooKeeper zk, String path) throws Exception {
        String[] list = path.split("/");
        String zkPath = "";
        for (int i = 0; i < list.length - 1; i++) {
            String str = list[i];
            if (StringUtils.isNotEmpty(str)) {
                zkPath = zkPath + "/" + str;
                if (zk.exists(zkPath, false) != null) {
                    byte[] value = zk.getData(zkPath, false, null);
                    if (value != null && new String(value).contains("uncode-schedule-")) {
                        throw new Exception("\"" + zkPath + "\"  is already a schedule instance's root directory, its any subdirectory cannot as the root directory of others");
                    }
                }
            }
        }
    }

    public List<ACL> getAcl() {
        return acl;
    }

    public ZooKeeper getZooKeeper() {
        try {
            if (!this.checkZookeeperState()) {
                reConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.zk;
    }

}