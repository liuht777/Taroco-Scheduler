# taroco-scheduler 分布式定时任务调度

![任务管理界面](/exts/%E4%BB%BB%E5%8A%A1%E7%AE%A1%E7%90%86%E7%95%8C%E9%9D%A2.jpg "任务管理界面")

基于 Spring Boot 2.0 + Spring Task + Zookeeper 的分布式任务调度组件，非常轻量级，使用简单，只需要引入jar包，不需要单独部署服务端。确保所有任务在集群中不重复，不遗漏的执行。

# 组件架构

![架构图](/exts/taroco%20scheduler%20架构图.jpg "架构图")

# 功能点概述

1. 每个任务在集群中不同节点上不重复的执行。
2. 单个任务节点故障时自动转移到其他任务节点继续执行。
3. 任务节点启动时必须保证zookeeper可用，任务节点运行期zookeeper集群不可用时任务节点保持可用前状态运行，zookeeper集群恢复正常运行。
4. 支持动态添加、修改和删除任务，支持任务暂停和重新启动。
5. 添加ip黑名单，过滤不需要执行任务的节点。
6. 提供后台管理界面和任务执行监控。
7. 天然支持 Spring Boot。通过 spring.factories 的方式自动加载配置类 `TarocoSchedulerAutoConfiguration`。只需要引入jar包依赖，无须显示的添加配置类扫描。
8. 支持单个任务运行多个实例（使用扩展后缀名）。
9. 本地静态任务（`@Scheduled` 注解的任务, 以及 xml 配置的定时任务），会自动纳入集群做统一分配执行。
10. 新增 server 节点, 当前任务会自动平衡到新节点。

## 特别说明：

* 单节点故障时需要业务保障数据完整性或幂等性。
* Spring Task 是Spring 3.0之后自主开发的定时任务工具。
* Spring Task 默认不是并行执行，需要添加一个名为 taskScheduler 的Bean，采用ThreadPoolTaskScheduler或其他线程池的Scheduler实现。Spring Task默认采用 ThreadPoolTaskScheduler
* 所有的任务都是基于Spring Bean的方式。可以通过定义一个或多个任务模板（Bean 的方式），通过使用任务后缀可以动态的添加多个该模板的任务实例，你只需要传递不同的参数即可。
* 此组件更加适用于，模板化的定时任务。我们可以事先定义很少的任务模板（Spring Bean），然后通过业务传递不同参数，指定后缀，批量生成定时任务。
* 虽然提供了统一的后端管理界面，但是依然更推荐通过定义任务模板的方式，将任务保存到本地数据库，自定义自己的任务管理界面。


------------------------------------------------------------------------

# 如何使用

## 基于Spring Boot的配置

一般情况下我们只需要指定 zookeeper 的地址即可:
```
taroco:
  scheduler:
    zk:
      url: 127.0.0.1:2181
```
如果需要更多的配置。请参考`TarocoSchedulerProperties`配置类。

## 启动任务管理界面
只需要在 Spring Boot 启动类中添加注解`@EnableTarocoSchedulerAdmin`，然后通过访问路径：`/taroco/scheduler/task`即可。

## 任务管理

注入 bean `IScheduleTask`即可实现对任务的增删查改。

## 节点管理

注入 bean `ISchedulerServer`即可实现对 server 节点的简单查询。

## 获取当前 server 节点实例
```
ScheduleServer.getInstance()
```
------------------------------------------------------------------------
