package io.github.liuht777.scheduler.config;

import io.github.liuht777.scheduler.SchedulerTaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
@EnableConfigurationProperties({TarocoSchedulerProperties.class})
public class TarocoSchedulerAutoConfiguration {

	private static final Logger LOGGER = LoggerFactory.getLogger(TarocoSchedulerAutoConfiguration.class);

	@Autowired
	private TarocoSchedulerProperties uncodeScheduleConfig;

	@Value("${server.port}")
	private String ownSign;

    @Bean(name = "taskScheduler", initMethod="init")
	public SchedulerTaskManager commonMapper(){
		SchedulerTaskManager schedulerTaskManager = new SchedulerTaskManager(ownSign);
		schedulerTaskManager.setConfig(uncodeScheduleConfig.getConfig());
        schedulerTaskManager.setThreadNamePrefix("UncodeSchedulerPool-");
		LOGGER.info("=====>SchedulerTaskManager inited..");
		return schedulerTaskManager;
	}
}
