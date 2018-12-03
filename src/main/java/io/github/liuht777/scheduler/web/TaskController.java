package io.github.liuht777.scheduler.web;

import io.github.liuht777.scheduler.constant.DefaultConstants;
import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.util.TaskUtil;
import io.github.liuht777.scheduler.vo.ServerVo;
import io.github.liuht777.scheduler.vo.TaskVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * task Controller
 *
 * @author liuht
 * 2018/11/28 16:31
 */
@Controller
@RequestMapping("/taroco/scheduler/task")
public class TaskController {

    @Autowired
    private ISchedulerServer schedulerServer;

    @Autowired
    private IScheduleTask scheduleTask;

    /**
     * 返回task页面
     *
     * @return
     */
    @GetMapping
    public String list(Model model) {
        final List<String> serverNames = schedulerServer.loadScheduleServerNames();
        final List<ServerVo> result = new ArrayList<>(serverNames.size());
        serverNames.forEach(name -> {
            final ServerVo server = new ServerVo();
            server.setName(name);
            server.setIsLeader(schedulerServer.isLeader(name, serverNames));
            result.add(server);
        });
        model.addAttribute("serverList", result);
        final List<Task> tasks = scheduleTask.selectTask();
        List<TaskVo> taskList = new ArrayList<>(tasks.size());
        tasks.forEach(task -> taskList.add(TaskVo.valueOf(task)));
        model.addAttribute("taskList", taskList);
        return "taroco/task";
    }

    /**
     * 新增任务
     *
     * @param vo 任务vo
     */
    @PostMapping
    @ResponseBody
    public void addTask(@RequestBody TaskVo vo) {
        final Task task = new Task();
        task.setTargetBean(vo.getTargetBean());
        task.setTargetMethod(vo.getTargetMethod());
        task.setCronExpression(vo.getCronExpression());
        task.setParams(vo.getParams());
        task.setExtKeySuffix(vo.getExtKeySuffix());
        task.setStartTime(new Date(System.currentTimeMillis()));
        task.setType(DefaultConstants.TYPE_TAROCO_TASK);
        scheduleTask.addTask(task);
    }

    /**
     * 删除任务
     *
     * @param stringKey 任务唯一key
     */
    @DeleteMapping("/{stringKey}")
    @ResponseBody
    public void removeTask(@PathVariable String stringKey) {
        scheduleTask.delTask(TaskUtil.valueOf(stringKey));
    }

    /**
     * 暂停/启动任务
     *
     * @param stringKey 任务唯一key
     * @param status 目标状态
     */
    @PutMapping("/{stringKey}/{status}")
    @ResponseBody
    public void statusChange(@PathVariable String stringKey,
                             @PathVariable String status) {
        final Task task = TaskUtil.valueOf(stringKey);
        task.setStatus(status);
        scheduleTask.updateTask(task);
    }
}
