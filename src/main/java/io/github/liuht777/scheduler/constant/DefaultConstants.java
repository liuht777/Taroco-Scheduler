package io.github.liuht777.scheduler.constant;

/**
 * 常量类
 *
 * @author liuht
 * 2018/11/21 9:41
 */
public interface DefaultConstants {

    /**
     * task节点名称
     */
    String NODE_TASK = "task";

    /**
     * server 节点名称
     */
    String NODE_SERVER = "server";

    /**
     * 任务类型: taroco scheduler 组件自定义任务
     */
    String TYPE_TAROCO_TASK = "taroco-task";

    /**
     * 任务类型: @Scheduled 注解的spring 任务
     */
    String TYPE_SPRING_TASK = "spring-task";

    /**
     * 任务运行状态: 错误
     */
    String STATUS_ERROR = "error";

    /**
     * 任务运行状态: 停止
     */
    String STATUS_STOP = "stop";

    /**
     * 任务运行状态: 正在运行
     */
    String STATUS_RUNNING = "running";
}
