package io.github.liuht777.scheduler.endpoint;

import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.http.MediaType;

import java.util.List;

/**
 * 定时任务task端点
 *
 * @author liuht
 * 2018/11/27 17:48
 */
@WebEndpoint(id = "taroco-scheduler-server")
public class SchedulerTaskEndpoint {

    @Autowired
    private IScheduleTask scheduleTask;

    /**
     * 查询task任务列表
     *
     * @return task任务列表
     */
    @ReadOperation(produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<Task> tasks() {
        return scheduleTask.selectTask();
    }
}
