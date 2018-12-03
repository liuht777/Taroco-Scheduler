package io.github.liuht777.scheduler.annotation;

import io.github.liuht777.scheduler.web.TaskController;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 启用定时任务后台管理界面
 *
 * @author liuht
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import({TaskController.class})
@Documented
public @interface EnableTarocoSchedulerAdmin {
}
