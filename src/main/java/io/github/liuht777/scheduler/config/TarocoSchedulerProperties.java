package io.github.liuht777.scheduler.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;

/**
 * 组件配置类
 *
 * @author liuht
 */
@ConfigurationProperties(prefix = "taroco.scheduler")
@Data
public class TarocoSchedulerProperties {

    /**
     * ip black list
     */
    private List<String> ipBlackList = Collections.emptyList();

    /**
     * spring task scheduler pool size, default: 10
     */
    private int poolSize = 10;

    /**
     * the interval for refresh & check tasks from zookeeper, default: 30 seconds
     */
    private int refreshTaskInterval = 30;

    /**
     * zookeeper properties
     */
    private Zk zk;

    @Data
    public static class Zk {

        /**
         * zookeeper connect String eg: 192.168.1.12:2181,192.168.1.13:2181,192.168.1.14:2181
         */
        private String url;

        /**
         * scheduler tool root path, default: /taroco/scheduler
         */
        private String rootPath = "/taroco/scheduler";

        /**
         * zookeeper session timeout, default: 3000 ms
         */
        private int sessionTimeout = 3000;

        /**
         * connect timeout, default: 3000 ms
         */
        private int connectionTimeout = 3000;
    }
}
