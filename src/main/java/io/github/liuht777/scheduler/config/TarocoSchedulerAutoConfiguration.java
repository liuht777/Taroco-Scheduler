package io.github.liuht777.scheduler.config;

import io.github.liuht777.scheduler.SchedulerTaskManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * SchedulerTaskManager配置
 *
 * @author liuht
 */
@Configuration
@EnableConfigurationProperties( {TarocoSchedulerProperties.class})
@Slf4j
public class TarocoSchedulerAutoConfiguration {

    @Value("${spring.application.name}")
    private String ownSign;

    /**
     * 定义调度核心管理器
     *
     * @param schedulerProperties
     * @return
     */
    @Bean(name = "taskScheduler", initMethod = "init")
    public SchedulerTaskManager schedulerTaskManager(TarocoSchedulerProperties schedulerProperties) {
        SchedulerTaskManager schedulerTaskManager = new SchedulerTaskManager(ownSign, schedulerProperties);
        schedulerTaskManager.setThreadNamePrefix("TarocoSchedulerPool-");
        log.info("=====>Taroco Scheduler TaskManager inited..");
        return schedulerTaskManager;
    }
}
