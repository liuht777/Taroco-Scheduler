package io.github.liuht777.scheduler.core;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

import static io.github.liuht777.scheduler.constant.DefaultConstants.STATUS_RUNNING;
import static io.github.liuht777.scheduler.constant.DefaultConstants.STATUS_STOP;

/**
 * 任务定义，提供关键信息给使用者
 *
 * @author liuht
 */
@Data
public class Task {

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
    private Date startTime;

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
     * 后台显示参数，当前任务执行节点
     */
    private String currentServer;

    /**
     * 后台显示参数，无业务内含
     */
    private int runTimes;

    /**
     * 后台显示参数，无业务内含
     */
    private long lastRunningTime;

    /**
     * 后台显示参数，无业务内含
     */
    private String status = STATUS_RUNNING;

    /**
     * 错误信息
     */
    private String errorMsg;

    /**
     * key的后缀
     */
    private String extKeySuffix;

    public void setTargetMethod(String targetMethod) {
        final String line = "-";
        if (StringUtils.isNotBlank(targetMethod) && targetMethod.contains(line)) {
            String[] vals = targetMethod.split(line);
            this.targetMethod = vals[0];
            this.extKeySuffix = vals[1];
        } else {
            this.targetMethod = targetMethod;
        }
    }

    public String getTargetMethod4Show() {
        if (StringUtils.isNotBlank(extKeySuffix)) {
            return targetMethod + "-" + extKeySuffix;
        }
        return targetMethod;
    }

    /**
     * 返回task 唯一key
     * @return
     */
    public String stringKey() {
        String result = null;
        boolean notBlank = StringUtils.isNotBlank(getTargetBean()) && StringUtils.isNotBlank(getTargetMethod());
        if (notBlank) {
            result = getTargetBean() + "#" + getTargetMethod();
        }
        if (StringUtils.isNotBlank(extKeySuffix)) {
            result += "-" + extKeySuffix;
        }
        return result;
    }

    public boolean isStop() {
        return STATUS_STOP.equals(this.status);
    }

    public void setStop() {
        this.status = STATUS_STOP;
    }

    public void valueOf(Task task) {
        if (StringUtils.isNotBlank(task.getTargetBean())) {
            this.targetBean = task.getTargetBean();
        }
        if (StringUtils.isNotBlank(task.getTargetMethod())) {
            if (task.getTargetMethod().contains("-")) {
                String[] vals = task.getTargetMethod().split("-");
                this.targetMethod = vals[0];
                this.extKeySuffix = vals[1];
            } else {
                this.targetMethod = task.getTargetMethod();
            }
        }
        if (StringUtils.isNotBlank(task.getCronExpression())) {
            this.cronExpression = task.getCronExpression();
        }
        if (task.getStartTime() != null) {
            this.startTime = task.getStartTime();
        }
        if (task.getPeriod() != null && task.getPeriod() > 0) {
            this.period = task.getPeriod();
        }
        if (StringUtils.isNotBlank(task.getParams())) {
            this.params = task.getParams();
        }
        if (StringUtils.isNotBlank(task.getType())) {
            this.type = task.getType();
        }
        if (StringUtils.isNotBlank(task.getStatus())) {
            this.status = task.getStatus();
        }
        if (StringUtils.isNotBlank(task.getExtKeySuffix())) {
            this.extKeySuffix = task.getExtKeySuffix();
        }
        if (StringUtils.isNotBlank(task.getErrorMsg())){
            this.errorMsg = task.getErrorMsg();
        }
    }
}
