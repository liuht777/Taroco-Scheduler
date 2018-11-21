# uncode-schedule

- 基于Spring Task + Zookeeper的分布式任务调度组件，非常小巧，使用简单，只需要引入jar包。不需要单独部署服务端。确保所有任务在集群中不重复，不遗漏的执行。支持动态添加和删除任务。
- [原文地址](https://gitee.com/uncode/uncode-schedule)，感兴趣的童鞋可以参考原版代码。

# 功能概述（包括优化的部分）

1. 基于zookeeper+spring task的分布任务调度系统。
2. 确保每个任务在集群中不同节点上不重复的执行。
3. 单个任务节点故障时自动转移到其他任务节点继续执行。
4. 任务节点启动时必须保证zookeeper可用，任务节点运行期zookeeper集群不可用时任务节点保持可用前状态运行，zookeeper集群恢复正常运行。
5. 支持动态添加、修改和删除任务，支持任务暂停和重新启动。
6. 添加ip黑名单，过滤不需要执行任务的节点。
7. 后台管理和任务执行监控。
8. 支持spring-boot,支持单个任务运行多个实例（使用扩展后缀）。

## 主要改动
1. 删除了quartz的集成。Spring Task和quartz都是一整套的定时任务框架，没有必要强行将quartz集成进来，专注做Spring Task的分布式以及动态任务的封装。删除quartz后，组件更加轻便。且所有功能依旧保留。
2. 对于Spring Boot的支持更加智能化。通过spring.factories的方式自动加载配置类UncodeScheduleAutoConfiguration。只需要引入jar包依赖，无须显示的添加配置类扫描。
3. 参照Alibaba代码规范对代码进行了大量重构优化，更具有可读性。
4. 删除了默认1s的心跳机制（主要作用：刷新server、重新分配任务、检查当前serve可执行的任务），采用watcher的方式，对server节点和task节点进行动态监听，达到分配任务的目的。重新分配任务会触发taskTrigger节点watch事件，解决集群环境下非leader节点不能动态检查本地任务的问题。进一步提升性能。
5. 关于重新分配任务，将之前的随机机制改为轮询机制。
6. 对于非动态添加的任务，也就是注解或配置文件配置的任务会在容器启动通过组件定义的方式启动。但是在删除此类任务时，没有真正的删除，taskWrapper任然会定时的执行。 解决了这个bug（tag 2.0.2中已经将本地静态任务@Scheduled 或 xml配置的定时任务从集群的动态管理中移除，关于本地静态任务可采用分布式锁的方式实现简单的分布式控制）。
7. 关于UncodeScheduleAutoConfiguration中SchedulerTaskManager的定义。将SchedulerTaskManager的Bean名称定义为taskScheduler，这样可以阻止Spring Task初始化名为taskScheduler的bean，以免重复加载。当然你也可以不这么做，因为SchedulerTaskManager继承了ThreadPoolTaskScheduler，我们动态添加的任务都是通过SchedulerTaskManager添加的。
8. 修复了原来的一些bug,优化了文档。代码实战中的代码，都是经过验证,运行正常。src/test中,有关于xml配置的测试用例。

说明：
* 组件依赖zookeeper。属性配置中zkUsername以及zkPassword并不是zk的账号密码，是默认任务管理界面的账号密码。
* 单节点故障时需要业务保障数据完整性或幂等性。
* Spring Task是Spring 3.0之后自主开发的定时任务工具。
* Spring Task默认不是并行执行，需要添加一个名为taskScheduler的Bean，采用ThreadPoolTaskScheduler或其他线程池的Scheduler实现。Spring Task默认采用ThreadPoolTaskScheduler
* 所有的任务都是基于Spring Bean的方式。可以通过定义一个或多个任务模板（Bean 的方式），通过使用任务后缀可以动态的添加多个该模板的任务实例，你只需要传递不同的参数即可。
* 经过改动后，此组件更加适用于，模板化的定时任务。我们可以事先定义很少的任务模板（Spring Bean），然后通过业务传递不同参数，指定后缀，批量生成定时任务。


------------------------------------------------------------------------

# 模块架构

![模块架构](http://git.oschina.net/uploads/images/2016/0513/180808_6a6c1046_277761.png "模块架构")
![Worker构成](http://git.oschina.net/uploads/images/2016/0513/180912_8c9a24ec_277761.png "Worker构成")


------------------------------------------------------------------------


# 代码实战

## 定义非动态的定时任务

```
@Component	
public class SimpleTask {

    private static int i = 0;
    
    @Scheduled(fixedDelay = 5000)
    public void print() {
    	System.out.println("===========start!=========");
    	System.out.println("I:"+i);i++;
    	System.out.println("=========== end !=========");
    }
    
    @Scheduled(cron = "0/5 * * * * ?")
    public void print1() {
    	System.out.println("===========start!=========");
    	System.out.println("I:"+i);i++;
    	System.out.println("=========== end !=========");
    }
    
    @Scheduled(fixedRate = 3000)
    public void print3() {
    	System.out.println("===========start!=========");
    	System.out.println("I:"+i);i++;
    	System.out.println("=========== end !=========");
    }
}
	
```
## 定义动态的定时任务

```
// 定义任务实体
TaskDefine task = new TaskDefine();
task.setTargetBean(SchedulerTaskForward.BEAN_NAME);
task.setTargetMethod(SchedulerTaskForward.METHOD);
task.setExtKeySuffix(SUFFIX + model.getId());
task.setCronExpression(model.getCronExpression());
task.setParams(gson.toJson(model));
if (ConsoleManager.isExistsTask(task)) {
    // 更新
    ConsoleManager.updateScheduleTask(task);
} else {
    // 新增
    task.setStartTime(new Date());
    ConsoleManager.addScheduleTask(task);
}
```
- SchedulerTaskForward是我预先定义好的任务模板，下面是代码片段

```
@SuppressWarnings("unchecked")
@Component("schedulerTaskForward")
public class SchedulerTaskForward {
    public static final String BEAN_NAME = "schedulerTaskForward";
    public static final String METHOD = "forward";
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerTaskForward.class);

    @Autowired
    private EpmManagerTerminalFeign terminalFeign;

    @Autowired
    private AmqpClientService mqService;

    @Value("${collectd.batch}")
    protected int batch;

    public void forward(String json) {
        SchedulerTaskModel model = gson.fromJson(json, SchedulerTaskModel.class);
        if (ExecuteType.report.getCode().equals(Integer.valueOf(model.getExecuteType()))) {
            // 上报的在定时任务中不执行
            throw new SchedulerTaskExecuteException("任务: " + model.getTaskName() + ", 属于上报任务, 禁止在定时任务中执行");
        }
        executeCollect(TaskType.forCode(model.getTaskType()), model.getAfns(), model.getOrgId(), model.getTermType());
        LOGGER.debug("任务: " + model.getTaskName() + ", 执行完成");
    }
```

## 基于Spring Boot的配置

1. application.yml

```
uncode:
  schedule:
    zkConnect: ${spring.cloud.zookeeper.connectString}
    rootPath: /uncode/schedule
    zkSessionTimeout: 60000
    zkUsername: admin
    zkPassword: admin
    poolSize: 10
#    ipBlackList[0]: 127.0.0.2 #server黑名单可选
#    ipBlackList[1]: 127.0.0.3 #server黑名单可选
```

	
2 启动类

```
@SpringBootApplication
@EnableScheduling
// 这个也是可选的,如果你不需要默认的任务管理界面的话(/uncode/schedule)
// 强烈建议自己去实现这个任务管理功能
@ServletComponentScan("cn.uncode.schedule")
public class UncodeScheduleApplication {
	public static void main(String[] agrs){
		SpringApplication.run(UncodeScheduleApplication.class,agrs);
	}
}
```

## 基于Spring项目配置

```
<!-- 分布式任务管理器 -->
<bean id="schedulerTaskManager" class="SchedulerTaskManager" init-method="init">
    <property name="config">
        <map>
            <entry key="uncode.schedule.zkConnect" value="127.0.0.1:2181" />
            <entry key="uncode.schedule.rootPath" value="/uncode/schedule" />
            <entry key="uncode.schedule.zkSessionTimeout" value="60000" />
            <entry key="uncode.schedule.zkUsername" value="admin" />
            <entry key="uncode.schedule.zkPassword" value="admin" />
            <entry key="uncode.schedule.poolSize" value="10" />
            <entry key="uncode.schedule.ipBlacklist" value="127.0.0.2,127.0.0.3" />
        </map>
    </property>
</bean>

<bean id="task1" class="TestTask" />

<task:scheduled-tasks scheduler="schedulerTaskManager">
    <task:scheduled ref="task1" method="task1" fixed-delay="5000"/>
</task:scheduled-tasks>
```

	
## 使用API或后台添加任务(静态方法的方式)

1 动态添加任务

ConsoleManager.addScheduleTask(TaskDefine taskDefine);

2 动态删除任务

ConsoleManager.delScheduleTask(TaskDefine taskDefine);

3 动态更新任务

ConsoleManager.updateScheduleTask(TaskDefine taskDefine);

4 查询任务列表

ConsoleManager.queryScheduleTask();

------------------------------------------------------------------------

## 使用API或后台添加任务(Spring Bean的方式)
通过获得我们定义的SchedulerTaskManager这个bean,依然可以动态的添加任务。这里就不展示了。

# 待优化的点
1. 动态扩展时重新分配任务。目前虽然已经改为了轮询的分配机制，但是前提是你要事先将集群中的每个节点都启动起来，当我们新加一个server的时候，不能将原有的任务进行重新分配。

# 关于

作者：刘惠涛  
转载请注明出处  
2017-10-23
