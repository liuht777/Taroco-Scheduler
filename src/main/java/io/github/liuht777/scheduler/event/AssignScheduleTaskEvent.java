package io.github.liuht777.scheduler.event;

import org.springframework.context.ApplicationEvent;

/**
 * 重新分配任务事件, 监听到此事件表明需要重新分配集群任务
 *
 * @author liuht
 * 2018/12/1 21:07
 */
public class AssignScheduleTaskEvent extends ApplicationEvent {

    private static final long serialVersionUID = -8598186881375129799L;

    /**
     * Create a new ApplicationEvent.
     *
     * @param source the object on which the event initially occurred (never {@code null})
     */
    public AssignScheduleTaskEvent(final Object source) {
        super(source);
    }
}
