package io.github.liuht777.scheduler.config;

import io.github.liuht777.scheduler.TaskManager;
import io.github.liuht777.scheduler.ThreadPoolTaskGenerator;
import io.github.liuht777.scheduler.core.IScheduleTask;
import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.rule.AssignServerRole;
import io.github.liuht777.scheduler.rule.impl.PollingAssignServerRole;
import io.github.liuht777.scheduler.zookeeper.ScheduleTaskZk;
import io.github.liuht777.scheduler.zookeeper.SchedulerServerZk;
import io.github.liuht777.scheduler.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_SERVER;
import static io.github.liuht777.scheduler.constant.DefaultConstants.NODE_TASK;

/**
 * SchedulerTaskManager配置
 *
 * @author liuht
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties( {TarocoSchedulerProperties.class})
@Slf4j
public class TarocoSchedulerAutoConfiguration {

    @Autowired
    private TarocoSchedulerProperties properties;

    /**
     * 定义 ISchedulerServer对象
     */
    @Bean
    public ISchedulerServer iSchedulerServer() {
        final String rootPath = properties.getZk().getRootPath();
        final String taskPath = rootPath + "/" + NODE_TASK;
        final String serverPath = rootPath + "/" + NODE_SERVER;
        return new SchedulerServerZk(serverPath, taskPath);
    }

    /**
     * 定义 IScheduleTask 对象
     */
    @Bean
    public IScheduleTask iScheduleTask() {
        final String rootPath = properties.getZk().getRootPath();
        final String taskPath = rootPath + "/" + NODE_TASK;
        return new ScheduleTaskZk(taskPath);
    }

    /**
     * 定义 server 选择策略
     */
    @Bean
    @ConditionalOnMissingBean(AssignServerRole.class)
    public AssignServerRole assignServerRole() {
        return new PollingAssignServerRole();
    }

    /**
     * 定义动态任务管理
     *
     */
    @Bean
    public TaskManager taskManager(ZkClient zkClient,
                                   AssignServerRole assignServerRole) {
        return new TaskManager(zkClient, assignServerRole);
    }

    /**
     * 定义定时任务生成器 bean名称必须为 taskScheduler
     */
    @Bean(name = "taskScheduler")
    public ThreadPoolTaskGenerator schedulerTaskManager(IScheduleTask scheduleTask,
                                                        ISchedulerServer schedulerServer) {
        final ThreadPoolTaskGenerator schedulerTaskManager = new ThreadPoolTaskGenerator(properties.getPoolSize(),
                scheduleTask, schedulerServer);
        schedulerTaskManager.setThreadNamePrefix("TarocoSchedulerPool-");
        return schedulerTaskManager;
    }

    /**
     * 定义 ZkClient 对象
     */
    @Bean
    public ZkClient zkClient(ThreadPoolTaskGenerator taskGenerator) {
        return new ZkClient(properties, taskGenerator);
    }
}
