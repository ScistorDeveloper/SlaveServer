package com.scistor.process.thrift.service;

import com.scistor.operator.DataParserOperator;
import com.scistor.operator.ZookeeperOperator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SlaveServiceImpl implements SlaveService.Iface {
	private static final Logger LOG= Logger.getLogger(SlaveServiceImpl.class);
	private List<ArrayBlockingQueue<Map>> queueList;
	private String zkPath;
	private String PRODUCE_PATH;
	private static String hostAddress;
	private static int PORT ;
	private static int QUEUE_SIZE;
	@Override
	public String addSubTask(List<Map<String, String>> elements, String taskId, int slaveNo, boolean consumer) throws TException {
		//初始化
		long tStart = System.currentTimeMillis();
		Properties props = new Properties();
		try {
			props.load(SlaveServiceImpl.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		PORT = Integer.parseInt(props.getProperty("thrift_slaveserver_port"));
		zkPath = props.getProperty("zookeeper_dir");
		String path = zkPath + slaveNo + "/" + taskId ;
		PRODUCE_PATH = path + "/is_produce";
		QUEUE_SIZE = Integer.parseInt(props.getProperty("queue_size"));
		System.out.println(elements.size());
		if(elements.size()<1) return "0";
		if(consumer){
			LOG.info("this is a machine include consumer");
		}
		//添加线程池
		ExecutorService threads= Executors.newFixedThreadPool(elements.size());
		//添加队列列表
		queueList = new ArrayList<ArrayBlockingQueue<Map>>();
		for (int i=0;i<elements.size()-1;i++){
			queueList.add(new ArrayBlockingQueue<Map>(QUEUE_SIZE));
		}
		//开始数据生产线程
		Thread producer=new Thread();
		try {
			File filedir = new File(elements.get(0).get("filedir"));
			DataParserOperator dp = new DataParserOperator();
			dp.init(filedir,queueList);
			producer = new Thread(dp);
			producer.start();
		}catch (Exception e){
			LOG.info(e.toString());
			e.printStackTrace();
		}
		//成功初始化，写zk目录
		try {
			ZookeeperOperator.registerSlaveInfo(path, taskId);
			ZookeeperOperator.registerSlaveInfo(PRODUCE_PATH,"producing flag");
		}catch (Exception e){
			LOG.info(e.toString());
			e.printStackTrace();
		}
		for(int i=1; i<elements.size(); i++) {
			//开始数据消费线程
			try {
				if(elements.get(i).get("task_type").equals("producer")) {
					HandleNewTask handnew = new HandleNewTask(elements.get(i), queueList.get(i - 1), PRODUCE_PATH);
					threads.execute(handnew);
				}else if(elements.get(i).get("task_type").equals("consumer")) {
					HandleNewTask handnew = new HandleNewTask(elements.get(i), queueList.get(i - 1), path);
					threads.execute(handnew);
				}
			}catch (Exception e){
				LOG.error(e.toString());
				e.printStackTrace();
			}
		}
		//等待执行结束
		//是否程序结束
		threads.shutdown();
		try {
			producer.join();
			System.out.println("producer exit");
			ZookeeperOperator.delete(PRODUCE_PATH,"");
			System.out.println(ZookeeperOperator.checkPath(PRODUCE_PATH));
		} catch (InterruptedException e) {
			LOG.info(e.toString());
			e.printStackTrace();
		} catch (Exception e) {
			LOG.info(e.toString());
			e.printStackTrace();
		}
		while (!threads.isTerminated());

		long tEnd = System.currentTimeMillis();
		long totalSeconds = (tEnd - tStart) / 1000;
		//求出现在的秒
		long currentSecond = totalSeconds % 60;
		//求出现在的分
		long totalMinutes = totalSeconds / 60;
		long currentMinute = totalMinutes % 60;
		//求出现在的小时
		long totalHour = totalMinutes / 60;
		long currentHour = totalHour % 24;
		System.out.println("总共用时:"+ currentHour+"h"+currentMinute+"m"+currentSecond + "s");
		LOG.info("总共用时:"+ currentHour+"h"+currentMinute+"m"+currentSecond + "s");
		//删除zk目录
		try {
			ZookeeperOperator.delete(path, taskId);
		}catch (Exception e){
			LOG.info(e.toString());
			e.printStackTrace();
		}

		//返回
		return null;
	}
}
