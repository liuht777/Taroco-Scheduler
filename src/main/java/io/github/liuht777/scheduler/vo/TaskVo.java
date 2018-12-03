package io.github.liuht777.scheduler.vo;

import io.github.liuht777.scheduler.core.Task;
import lombok.Data;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * task VO
 *
 * @author liuht
 * 2018/11/29 16:52
 */
@Data
public class TaskVo {

    /**
     * 任务唯一key
     */
    private String key;

    /**
     * 目标bean
     */
    private String targetBean;

    /**
     * 目标方法
     */
    private String targetMethod;

    /**
     * cron表达式
     */
    private String cronExpression;

    /**
     * 开始时间
     */
    private String startTime;

    /**
     * 周期（毫秒）
     */
    private Long period;

    /**
     * 参数
     */
    private String params;

    /**
     * 类型
     */
    private String type;

    /**
     * 当前分配server节点
     */
    private String currentServer;

    /**
     * 运行次数
     */
    private int runTimes;

    /**
     * 最后运行时间
     */
    private String lastRunningTime;

    /**
     * 运行状态
     */
    private String status;

    /**
     * key的后缀
     */
    private String extKeySuffix;

    /**
     * 错误信息
     */
    private String errorMsg;

    /**
     * task 转 taskVo
     *
     * @param task
     * @return
     */
    public static TaskVo valueOf(Task task) {
        final TaskVo taskVo = new TaskVo();
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        taskVo.setKey(task.stringKey());
        taskVo.setTargetBean(task.getTargetBean());
        taskVo.setTargetMethod(task.getTargetMethod());
        taskVo.setCronExpression(task.getCronExpression());
        taskVo.setStartTime(format.format(task.getStartTime()));
        taskVo.setPeriod(task.getPeriod());
        taskVo.setParams(task.getParams());
        taskVo.setLastRunningTime(format.format(new Date(task.getLastRunningTime())));
        taskVo.setCurrentServer(task.getCurrentServer());
        taskVo.setRunTimes(task.getRunTimes());
        taskVo.setStatus(task.getStatus());
        taskVo.setExtKeySuffix(task.getExtKeySuffix());
        taskVo.setErrorMsg(task.getErrorMsg());
        taskVo.setType(task.getType());
        return taskVo;
    }
}
