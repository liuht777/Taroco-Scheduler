package io.github.liuht777.scheduler.web;

import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.core.Task;
import io.github.liuht777.scheduler.vo.ServerVo;
import io.github.liuht777.scheduler.vo.TaskVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
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
}
