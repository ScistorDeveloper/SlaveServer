package com.scistor.process;
import com.scistor.operator.DataParserOperator;
import com.scistor.operator.FlumeClientOperator;
import com.scistor.operator.ZookeeperOperator;
import com.scistor.process.thrift.service.SlaveServiceImpl;
import com.scistor.utils.ExceptionHandler;
import com.scistor.utils.TaskResult;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static com.scistor.operator.DataParserOperator.getFileList;

public class test {
    public static void main(String[] args) throws Exception {

//        List<File> filelist = getFileList("","E:\\Project1\\HS\\02 数据资料\\170811\\");
//        System.out.println(filelist.size());
//        System.exit(2);
        //开始数据生产线程
        List<Map<String, String>> elements = new ArrayList<Map<String, String>>();

        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.DataParserOperator");
//                put("filedir","E:\\01\\");
                put("filedir","E:\\Project1\\HS\\02 数据资料\\170811\\");
                put("taskId","76c69097-57ef-4128-b708-8704f1ff3ca8");
            }
        });
        List<ArrayBlockingQueue<Map>> qlist = new ArrayList<ArrayBlockingQueue<Map>>();
        qlist.add(new ArrayBlockingQueue<Map>(20000));
        Thread producer=new Thread();
        DataParserOperator dp = new DataParserOperator();
        dp.init(elements.get(0),qlist);
        producer = new Thread(dp);
        producer.setUncaughtExceptionHandler(new ExceptionHandler());
        producer.start();
        producer.join();

//        String task_id = "76c69097-57ef-4128-b708-8704f1ff3ca8";
//        String mainclass = "com.scistor.operator.DataParserOperator";
//        TaskResult tr = new TaskResult(task_id,mainclass,true,"test");
//        ZooKeeper zk = ZookeeperOperator.getZk();
//        ZookeeperOperator.updateTaskResult(zk,null,task_id,mainclass,tr);
//        zk.close();
        System.out.println("end");

    }


}
