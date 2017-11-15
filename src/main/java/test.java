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
        Properties props = new Properties();
        props.put("client.type", "default_failover");

// List of hosts (space-separated list of user-chosen host aliases)
        props.put("hosts", "h1 h2 h3");

// host/port pair for each host alias
        String host1 = "host1.example.org:41414";
        String host2 = "host2.example.org:41414";
        String host3 = "host3.example.org:41414";
        props.put("hosts.h1", host1);
        props.put("hosts.h2", host2);
        props.put("hosts.h3", host3);
        System.out.println(props.toString());

        //zookeeper test
//        System.out.println(ZookeeperOperator.checkPath("/zookeeper/01"));
//        ZookeeperOperator.registerSlaveInfo("/zookeeper/01","aaaaaaaaaaa","PERSISTENT");
//        ZookeeperOperator.delete("/zookeeper/01","aaaaaaaaaaa");
////        ZookeeperOperator.registerSlaveInfo("/HS/1","aaaaaaaaaaa");
////        ZookeeperOperator.registerSlaveInfo("/HS/1/1","aaaaaaaaaaa");
////        ZookeeperOperator.registerSlaveInfo("/HS/1/1/produce","aaaaaaaaaaa");
//        System.out.println(ZookeeperOperator.getZk().getChildren("/HS/1/1/produce",null));
//        Stat stat = new Stat();
//        System.out.println(ZookeeperOperator.getZk().getData("/HS/1/1/produce",null,stat));
//        System.out.println(ZookeeperOperator.getZk().setData("/HS/1/1/produce",null,-1));
//        System.out.println(stat.getAversion());
//        ZookeeperOperator.delete("/HS/1/1/produce","aaaaaaaaaaa");
//        System.out.println(ZookeeperOperator.checkPath("/HS/1/1/produce"));
//        //flume client test
//        for(int i=0; i<10; i++) {
//            try {
//                FlumeClientOperator.sendDataToFlume("this is a client test");
////                Thread.sleep(1000);
//                FlumeClientOperator.sendDataToFlume("this is a client test");
//            }catch (Exception e){
//                System.out.println("flume client error");
//                e.printStackTrace();
//            }
////            System.out.println("send" + i);
//        }
//        FlumeClientOperator.cleanUp();
//        System.exit(0);
//        File dir = new File("sdf");
//        System.out.println(dir.isDirectory());
//
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
