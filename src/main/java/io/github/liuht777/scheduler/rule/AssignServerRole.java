package io.github.liuht777.scheduler.rule;

import java.util.List;

/**
 * 分配任务选择server策略
 *
 * @author liuht
 * 2018/12/3 16:05
 */
public interface AssignServerRole {


    /**
     * 根据 server 列表选择 server
     *
     * @param serverList server 列表
     * @return 目标server
     */
    String doSelect(List<String> serverList);
}
