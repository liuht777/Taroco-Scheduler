package io.github.liuht777.scheduler.event;

import org.springframework.context.ApplicationEvent;

/**
 * server 节点新增事件
 *
 * @author liuht
 * 2018/12/3 16:28
 */
public class ServerNodeAddEvent extends ApplicationEvent {

    private static final long serialVersionUID = 4445267270378660677L;

    /**
     * Create a new ApplicationEvent.
     *
     * @param source the object on which the event initially occurred (never {@code null})
     */
    public ServerNodeAddEvent(final Object source) {
        super(source);
    }
}
