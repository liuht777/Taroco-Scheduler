package io.github.liuht777.scheduler.util;

import io.github.liuht777.scheduler.core.Task;
import org.apache.commons.lang3.StringUtils;

/**
 * @author liuht
 */
public final class TaskUtil {

    /**
     * 解析task
     *
     * @param stringKey task唯一key
     * @return Task
     */
    public static Task valueOf(final String stringKey) {
        if (StringUtils.isEmpty(stringKey)) {
            throw new RuntimeException("Task stringKey can not be empty");
        }
        final String[] split = stringKey.split("#");
        final Task task = new Task();
        task.setTargetBean(split[0]);
        final String[] ss = split[1].split("-");
        task.setTargetMethod(ss[0]);
        if (ss.length > 1 && StringUtils.isNotEmpty(ss[1])) {
            task.setExtKeySuffix(ss[1]);
        }
        return task;
    }
}
