# SlaveServer

* com.scistor.process.thrift.server.StartSlaveServer
    - PRC服务主类，配置文件为config.properties
    - 运行时创建/HS/slave/IP:PORT目录供远程调用使用。
* com.scistor.process.thrift.service.SlaveServiceImpl
    - PRC服务调用实现，实现addTask()函数，通过解析远程调用的参数，解析任务运行。
		由于每个子程序之间是异步结束，并且存在依赖关系，所以线程安排如下：
      - 数据解析程序，独立线程，成功初始化并行后创建/HS/slave/IP:PORT/is_produce目录，供算子监控，数据解析结束，将目录删除。解析的数据放到每个算子的消息队列供使用。
      - 算子生产部分，由producer标识，放入线程池中。由消息队列取数据，如果数据消费完且/HS/slave/IP:PORT/is_produce目录不存在，则退出。
      - 算子消费部分，由consumer标识，独立线程。由kafka取数据，监控所有子节点的算子生产者是否退出。
      - 程序运行过程中的异常被写到/HS/tasks/taskid/IP:PORT/mainclass/目录下。
      
* com.scistor.process.thrift.service.HandleNewTask
    - 算子调用主类，通过反射动态加载算子主类并运行

* com.scistor.operator.*
    - DataParserOperator:数据解析类
    - FlumeClientOperator:Flume Client类
    - ZookeeperOperator:Zookeeper类
* com.scistor.utils.*
    - 工具类
