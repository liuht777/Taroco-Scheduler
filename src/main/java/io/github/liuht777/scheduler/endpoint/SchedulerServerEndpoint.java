package io.github.liuht777.scheduler.endpoint;


import io.github.liuht777.scheduler.core.ISchedulerServer;
import io.github.liuht777.scheduler.vo.ServerVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.http.MediaType;

import java.util.ArrayList;
import java.util.List;

/**
 * 定时任务server端点
 *
 * @author liuht
 * 2018/11/27 15:44
 */
@WebEndpoint(id = "taroco-scheduler-server")
public class SchedulerServerEndpoint {

    @Autowired
    private ISchedulerServer schedulerServer;

    /**
     * 查询server节点
     *
     * @return server节点list
     */
    @ReadOperation(produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<ServerVo> servers() {
        final List<String> serverNames = schedulerServer.loadScheduleServerNames();
        final List<ServerVo> result = new ArrayList<>(serverNames.size());
        serverNames.forEach(name -> {
            final ServerVo server = new ServerVo();
            server.setName(name);
            server.setIsLeader(schedulerServer.isLeader(name, serverNames));
            result.add(server);
        });
        return result;
    }
}
