package com.scistor.process.thrift.service;

import com.scistor.operator.DataParserOperator;
import com.scistor.operator.FlumeClientOperator;
import com.scistor.operator.KafkaConsumerOperator;
import com.scistor.operator.ZookeeperOperator;
import com.scistor.utils.ExceptionHandler;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
	public String addSubTask(List<Map<String, String>> elements, String taskId, String slaveNo, boolean consumer) throws TException {
		//初始化
		long tStart = System.currentTimeMillis();
		Properties props = new Properties();
		try {
			props.load(SlaveServiceImpl.class.getClassLoader().getResourceAsStream("config.properties"));
			PORT = Integer.parseInt(props.getProperty("thrift_slaveserver_port"));
			zkPath = props.getProperty("thrift_slaveserver_dir");
			String path = zkPath + slaveNo + "/" + taskId ;
			PRODUCE_PATH = path + "/is_produce";
			QUEUE_SIZE = Integer.parseInt(props.getProperty("queue_size"));
			System.out.println(elements.size());
			if(elements.size()<1) return "0";
			if(consumer){
				LOG.info("this is a machine include consumer");
			}
			test_Showelements(elements);
			//添加算子生产数据线程池
			ExecutorService threads= Executors.newFixedThreadPool(elements.size());
			//添加算子消费数据线程
			FutureTask<String> task = null;
			//添加队列列表
			queueList = new ArrayList<ArrayBlockingQueue<Map>>();
			for (int i=0;i<(consumer?elements.size()-2:elements.size()-1);i++){
				queueList.add(new ArrayBlockingQueue<Map>(QUEUE_SIZE));
			}
			System.out.println("create queue list size " + queueList.size());
			//开始数据生产线程
			Thread producer=new Thread();
			DataParserOperator dp = new DataParserOperator();
			dp.init(elements.get(0),queueList);
			producer = new Thread(dp);
			producer.setUncaughtExceptionHandler(new ExceptionHandler());
			producer.start();
			//成功初始化，写zk目录
			ZookeeperOperator.registerSlaveInfo(path, taskId, "PERSISTENT");
			ZookeeperOperator.registerSlaveInfo(PRODUCE_PATH,"producing flag","PERSISTENT");
			List<Future> futureList = new ArrayList<Future>();
			for(int i=1; i<elements.size(); i++) {
				//开始数据消费线程
				try {
					if(elements.get(i).get("task_type").equals("producer")) {
						HandleNewTask handnew = new HandleNewTask(elements.get(i), queueList.get(i - 1), PRODUCE_PATH);
						futureList.add(threads.submit(handnew));
					}else if(elements.get(i).get("task_type").equals("consumer")) {
						//设置消费线程
						HandleNewTask handnew = new HandleNewTask(elements.get(i), null, path);
						task = new FutureTask<String>(handnew);
						new Thread(task, "有返回值的线程").start();
					}
				}catch (Exception e){
					LOG.error(e.toString());
					throw new RuntimeException(e);
				}
			}
			//等待执行结束
			//是否程序结束
			threads.shutdown();
			producer.join();
			System.out.println("producer exit");
			ZookeeperOperator.delete(PRODUCE_PATH,"");
			System.out.println(ZookeeperOperator.checkPath(PRODUCE_PATH));
			for(Future f : futureList){
				try{
					f.get();
				}catch (Exception e){
					System.out.println("this Main  and Future captured a new exception ");
					e.printStackTrace();
					//export exception
					return e.toString();
				}
			}
			while (!threads.isTerminated()){
				System.out.println(".");
				Thread.sleep(1000);
			}

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
			ZookeeperOperator.delete(path, taskId);
			//监控算子消费线程是否完成
			try {
				// 获取线程返回值
				System.out.println("子线程的返回值：" + task.get());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			//返回
			System.out.println("SLAVER WORKING IS DONE!!!");
			return "success";

		}catch (Exception e){
			e.printStackTrace();
			LOG.error(e.getMessage());
			System.out.println("we are going to clean env and exist...");
			//CLEARN SYSTEM
			try {
				//删除zk目录
				String path = zkPath + slaveNo + "/" + taskId ;
				ZookeeperOperator.delete(path, taskId);
				ZookeeperOperator.getZk().close();
				FlumeClientOperator.cleanUp();
//				KafkaConsumerOperator
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (Exception e1){
				e1.printStackTrace();
			}
			return(e.toString());
		}

	}

	private void test_Showelements(List<Map<String, String>> elements) {
		for(Map<String,String> e : elements){
			System.out.println(e.toString());
		}
	}

	@Override
	public String updateClassLoader(String componentLocation, String componentName, ByteBuffer componentInfo) throws TException {
		FileOutputStream fos;
		try {
			File file = new File(componentLocation + File.separator + componentName + "_sub.jar");
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			fos = new FileOutputStream(file);
			fos.write(componentInfo.array());
			fos.flush();
			fos.close();
		} catch (Exception e) {
			LOG.error(e);
			return "-100";
		}
		return "0";
	}
}
