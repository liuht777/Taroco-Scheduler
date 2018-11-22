# taroco-scheduler 分布式定时任务调度

基于 Spring Boot 2.0 + Spring Task + Zookeeper 的分布式任务调度组件，非常轻量级，使用简单，只需要引入jar包，不需要单独部署服务端。确保所有任务在集群中不重复，不遗漏的执行。

# 功能点概述

1. 每个任务在集群中不同节点上不重复的执行。
2. 单个任务节点故障时自动转移到其他任务节点继续执行。
3. 任务节点启动时必须保证zookeeper可用，任务节点运行期zookeeper集群不可用时任务节点保持可用前状态运行，zookeeper集群恢复正常运行。
4. 支持动态添加、修改和删除任务，支持任务暂停和重新启动。
5. 添加ip黑名单，过滤不需要执行任务的节点。
6. 提供后台管理界面和任务执行监控，需要添加注解 `@ServletComponentScan("io.github.liuht777.scheduler.web")`。
7. 天然支持 Spring Boot。通过 spring.factories 的方式自动加载配置类 `TarocoSchedulerAutoConfiguration`。只需要引入jar包依赖，无须显示的添加配置类扫描。
8. 支持单个任务运行多个实例（使用扩展后缀名）。
9. 本地静态任务（`@Scheduled` 注解的任务, 以及 xml 配置的定时任务），不纳入集群管理的范畴。

## 特别说明：

* 单节点故障时需要业务保障数据完整性或幂等性。
* Spring Task 是Spring 3.0之后自主开发的定时任务工具。
* Spring Task 默认不是并行执行，需要添加一个名为 taskScheduler 的Bean，采用ThreadPoolTaskScheduler或其他线程池的Scheduler实现。Spring Task默认采用 ThreadPoolTaskScheduler
* 所有的任务都是基于Spring Bean的方式。可以通过定义一个或多个任务模板（Bean 的方式），通过使用任务后缀可以动态的添加多个该模板的任务实例，你只需要传递不同的参数即可。
* 经过改动后，此组件更加适用于，模板化的定时任务。我们可以事先定义很少的任务模板（Spring Bean），然后通过业务传递不同参数，指定后缀，批量生成定时任务。


------------------------------------------------------------------------

# 模块架构

------------------------------------------------------------------------


# 代码实战

## 定义非动态的定时任务


## 定义动态的定时任务

## 基于Spring Boot的配置

1. application.yml

2 启动类


## 基于Spring项目配置


## 使用API或后台添加任务(静态方法的方式)

1 动态添加任务

DynamicTaskHelper.addScheduleTask(TaskDefine task);

2 动态删除任务

DynamicTaskHelper.delScheduleTask(TaskDefine task);

3 动态更新任务

DynamicTaskHelper.updateScheduleTask(TaskDefine task);

4 查询任务列表

DynamicTaskHelper.queryScheduleTask();

------------------------------------------------------------------------

# 待优化的点
1. 动态扩展时重新分配任务。目前虽然已经改为了轮询的分配机制，但是前提是你要事先将集群中的每个节点都启动起来，当我们新加一个server的时候，不能将原有的任务进行重新分配。

# 关于

作者：刘惠涛  
转载请注明出处  
2018-11-22
