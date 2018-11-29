package io.github.liuht777.scheduler.vo;

import lombok.Data;

/**
 * 服务节点vo
 *
 * @author liuht
 * 2018/11/27 16:47
 */
@Data
public class ServerVo {

    /**
     * 节点名称
     */
    private String name;

    /**
     * 是否是leader节点
     */
    private Boolean isLeader;
}
