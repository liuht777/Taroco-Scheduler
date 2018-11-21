package io.github.liuht777.scheduler.util;

import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;


/**
 * 调度处理工具类
 *
 * @author liuht
 */
public class ScheduleUtil {

    /**
     * 获取本地 host
     */
    public static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取本地ip
     *
     * @return
     */
    public static String getLocalIP() {
        // 本地IP，如果没有配置外网IP则返回它
        String localip = null;
        // 外网IP
        String netip = null;
        Enumeration<NetworkInterface> netInterfaces;
        try {
            netInterfaces = NetworkInterface
                    .getNetworkInterfaces();
        } catch (SocketException e) {
            e.printStackTrace();
            return null;
        }
        InetAddress ip;
        // 是否找到外网IP
        boolean finded = false;
        while (netInterfaces.hasMoreElements() && !finded) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                ip = address.nextElement();
                if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress()
                        && !ip.getHostAddress().contains(":")) {
                    // 外网IP
                    netip = ip.getHostAddress();
                    finded = true;
                    break;
                } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress()
                        && !ip.getHostAddress().contains(":")) {
                    // 内网IP
                    localip = ip.getHostAddress();
                }
            }
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }

    /**
     * 构建任务key
     *
     * @param beanName     bean 名称
     * @param methodName   方法名称
     * @param extKeySuffix key后缀
     * @return
     */
    public static String buildScheduleKey(String beanName, String methodName, String extKeySuffix) {
        String result = beanName + "#" + methodName;
        if (StringUtils.isNotBlank(extKeySuffix)) {
            result += "-" + extKeySuffix;
        }
        return result;
    }


    /**
     * 构建任务key
     *
     * @param beanName   bean 名称
     * @param methodName 方法名称
     * @return
     */
    public static String buildScheduleKey(String beanName, String methodName) {
        return buildScheduleKey(beanName, methodName, null);
    }

    /**
     * 分配任务数量
     *
     * @param serverNum         总的服务器数量
     * @param taskItemNum       任务项数量
     * @param maxNumOfOneServer 每个server最大任务项数目
     * @return null
     */
    public static int[] assignTaskNumber(int serverNum, int taskItemNum, int maxNumOfOneServer) {
        int[] taskNums = new int[serverNum];
        int numOfSingle = taskItemNum / serverNum;
        int otherNum = taskItemNum % serverNum;
        if (maxNumOfOneServer > 0 && numOfSingle >= maxNumOfOneServer) {
            numOfSingle = maxNumOfOneServer;
            otherNum = 0;
        }
        for (int i = 0; i < taskNums.length; i++) {
            if (i < otherNum) {
                taskNums[i] = numOfSingle + 1;
            } else {
                taskNums[i] = numOfSingle;
            }
        }
        return taskNums;
    }
}
