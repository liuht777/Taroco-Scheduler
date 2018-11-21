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
     * 最后一次心跳通知时间
     */
    private Timestamp heartBeatTime;

    /**
     * 最后一次取数据时间
     */
    private Timestamp lastFetchDataTime;

    /**
     * 处理描述信息，例如读取的任务数量，处理成功的任务数量，处理失败的数量，处理耗时
     * FetchDataCount=4430,FetcheDataNum=438570,DealDataSucess=438570,DealDataFail=0,DealSpendTime=651066
     */
    private String dealInfoDesc;

    /**
     * 下次执行时间
     */
    private String nextRunStartTime;

    /**
     * 下次执行结束时间
     */
    private String nextRunEndTime;

    /**
     * 配置中心的当前时间
     */
    private Timestamp centerServerTime;

    /**
     * 数据版本号
     */
    private long version;

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
        result.heartBeatTime = null;
        result.dealInfoDesc = "调度初始化";
        result.version = 0;
        result.uuid = result.ip
                + "$"
                + (UUID.randomUUID().toString().replaceAll("-", "")
                .toUpperCase());
        return result;
    }
}
