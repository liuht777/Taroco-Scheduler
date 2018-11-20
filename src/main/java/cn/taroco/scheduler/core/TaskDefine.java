package cn.taroco.scheduler.core;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

/**
 * 任务定义，提供关键信息给使用者
 * @author juny.ye
 *
 */
public class TaskDefine {

	public static final String TYPE_UNCODE_TASK = "uncode-task";
	public static final String TYPE_SPRING_TASK = "spring-task";

	public static final String STATUS_ERROR = "error";
	public static final String STATUS_STOP = "stop";
	public static final String STATUS_RUNNING = "running";

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
	private long period;

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
	 * key的后缀
	 */
	private String extKeySuffix;


	public boolean begin(Date sysTime) {
		return null != sysTime && sysTime.after(startTime);
	}

	public String getTargetBean() {
		return targetBean;
	}

	public void setTargetBean(String targetBean) {
		this.targetBean = targetBean;
	}

	public String getTargetMethod() {
		return targetMethod;
	}

	public String getTargetMethod4Show(){
		if(StringUtils.isNotBlank(extKeySuffix)){
			return targetMethod + "-" + extKeySuffix;
		}
		return targetMethod;
	}

	public void setTargetMethod(String targetMethod) {
		if(StringUtils.isNotBlank(targetMethod) && targetMethod.indexOf("-") != -1){
			String[] vals = targetMethod.split("-");
			this.targetMethod = vals[0];
			this.extKeySuffix = vals[1];
		}else{
			this.targetMethod = targetMethod;
		}
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	public String getCurrentServer() {
		return currentServer;
	}

	public void setCurrentServer(String currentServer) {
		this.currentServer = currentServer;
	}

	public String stringKey(){
		String result = null;
		boolean notBlank = StringUtils.isNotBlank(getTargetBean()) && StringUtils.isNotBlank(getTargetMethod());
		if(notBlank){
			result = getTargetBean() + "#" + getTargetMethod();
		}
		if(StringUtils.isNotBlank(extKeySuffix)){
			result += "-" + extKeySuffix;
		}
		return result;
	}

	public String getParams() {
		return params;
	}

	public void setParams(String params) {
		this.params = params;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getRunTimes() {
		return runTimes;
	}

	public void setRunTimes(int runTimes) {
		this.runTimes = runTimes;
	}

	public long getLastRunningTime() {
		return lastRunningTime;
	}

	public void setLastRunningTime(long lastRunningTime) {
		this.lastRunningTime = lastRunningTime;
	}

	public boolean isStop() {
		return STATUS_STOP.equals(this.status);
	}

	public void setStop() {
		this.status = STATUS_STOP;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getExtKeySuffix() {
		return extKeySuffix;
	}

	public void setExtKeySuffix(String extKeySuffix) {
		this.extKeySuffix = extKeySuffix;
	}

	public void valueOf(TaskDefine taskDefine){
		if(StringUtils.isNotBlank(taskDefine.getTargetBean())){
			this.targetBean = taskDefine.getTargetBean();
		}
		if(StringUtils.isNotBlank(taskDefine.getTargetMethod())){
			if(taskDefine.getTargetMethod().contains("-")){
				String[] vals = taskDefine.getTargetMethod().split("-");
				this.targetMethod = vals[0];
				this.extKeySuffix = vals[1];
			}else{
				this.targetMethod = taskDefine.getTargetMethod();
			}
		}
		if(StringUtils.isNotBlank(taskDefine.getCronExpression())){
			this.cronExpression = taskDefine.getCronExpression();
		}
		if(taskDefine.getStartTime() != null){
			this.startTime = taskDefine.getStartTime();
		}
		if(taskDefine.getPeriod() > 0){
			this.period = taskDefine.getPeriod();
		}
		if(StringUtils.isNotBlank(taskDefine.getParams())){
			this.params = taskDefine.getParams();
		}
		if(StringUtils.isNotBlank(taskDefine.getType())){
			this.type = taskDefine.getType();
		}
		if(StringUtils.isNotBlank(taskDefine.getStatus())){
			this.status = taskDefine.getStatus();
		}
		if(StringUtils.isNotBlank(taskDefine.getExtKeySuffix())){
			this.extKeySuffix = taskDefine.getExtKeySuffix();
		}
	}


    @Override
	public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((targetBean == null) ? 0 : targetBean.hashCode());
        result = prime * result + ((targetMethod == null) ? 0 : targetMethod.hashCode());
        result = prime * result + ((cronExpression == null) ? 0 : cronExpression.hashCode());
        result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
        result = prime * result + (int)period;
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((extKeySuffix == null) ? 0 : extKeySuffix.hashCode());
        return result;
    }

	@Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TaskDefine)) {
            return false;
        }
        TaskDefine ou = (TaskDefine) obj;
        if (!ObjectUtils.equals(this.targetBean, ou.targetBean)) {
            return false;
        }
        if (!ObjectUtils.equals(this.targetMethod, ou.targetMethod)) {
            return false;
        }
        if (!ObjectUtils.equals(this.cronExpression, ou.cronExpression)) {
            return false;
        }
        if (!ObjectUtils.equals(this.startTime, ou.startTime)) {
            return false;
        }
        if (!ObjectUtils.equals(this.params, ou.params)) {
            return false;
        }
        if (!ObjectUtils.equals(this.type, ou.type)) {
            return false;
        }
        if (!ObjectUtils.equals(this.extKeySuffix, ou.extKeySuffix)) {
            return false;
        }
        return this.period == ou.period;
    }




}
