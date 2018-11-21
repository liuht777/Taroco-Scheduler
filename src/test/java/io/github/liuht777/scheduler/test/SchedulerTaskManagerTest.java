package io.github.liuht777.scheduler.test;

import io.github.liuht777.scheduler.ConsoleManager;
import io.github.liuht777.scheduler.SchedulerTaskManager;
import io.github.liuht777.scheduler.core.TaskDefine;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;

/**
 * SchedulerTaskManager
 *
 * @author liuht
 * @date 2017/10/23 15:10
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SchedulerTaskManagerTest {
    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void testXml() throws InterruptedException {
        Assert.assertNotNull(applicationContext);
        SchedulerTaskManager taskManager = (SchedulerTaskManager) applicationContext.getBean("schedulerTaskManager");
        System.out.println(taskManager.getScheduleTask().isRunning("task#task1"));
        Thread.sleep(60000);
    }

    @Test
    public void testAdd() throws InterruptedException {
        // 定义任务实体
        TaskDefine task = new TaskDefine();
        task.setTargetBean("task");
        task.setTargetMethod("task2");
        task.setExtKeySuffix(null);
        task.setPeriod(5000);
        if (ConsoleManager.isExistsTask(task)) {
            // 更新
            ConsoleManager.updateScheduleTask(task);
        } else {
            // 新增
            task.setStartTime(new Date());
            ConsoleManager.addScheduleTask(task);
        }
        Thread.sleep(60000);
    }
}
