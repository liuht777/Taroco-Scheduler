package io.github.liuht777.scheduler.core;

import io.github.liuht777.scheduler.util.ScheduleUtil;
import lombok.Data;

import java.sql.Timestamp;
import java.util.UUID;


/**
 * 调度服务器信息定义
 *
 * @author liuht
 */
@Data
public class ScheduleServer {
    /**
     * 全局唯一编号
     */
    private String uuid;

    /**
     * server标识 用于标记server独立性 避免重复注册
     */
    private String ownSign;

    /**
     * 机器IP地址
     */
    private String ip;

    /**
     * 机器名称
     */
    private String hostName;

    /**
     * 服务开始时间
     */
    private Timestamp registerTime;

    /**
     * 是否注册到server
     */
    private boolean isRegister;

    public ScheduleServer() {

    }

    /**
     * 创建分布式任务server对象
     *
     * @param aOwnSign server标识
     * @return
     */
    public static ScheduleServer createScheduleServer(String aOwnSign) {
        long currentTime = System.currentTimeMillis();
        ScheduleServer result = new ScheduleServer();
        result.ownSign = aOwnSign;
        result.ip = ScheduleUtil.getLocalIP();
        result.hostName = ScheduleUtil.getLocalHostName();
        result.registerTime = new Timestamp(currentTime);
        result.uuid = result.ip
                + "$"
                + (UUID.randomUUID().toString().replaceAll("-", "")
                .toUpperCase());
        return result;
    }
}
